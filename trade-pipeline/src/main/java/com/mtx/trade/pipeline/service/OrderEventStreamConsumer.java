package com.mtx.trade.pipeline.service;

import com.mtx.trade.pipeline.config.OrderEventConsumerProperties;
import com.mtx.trade.pipeline.dto.OrderEventMessage;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.time.LocalDateTime;

/** 订单事件消费、跨实例 PEL 接管以及 Redis ACK 编排。 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(
        prefix = "trade.pipeline.order-event-consumer",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class OrderEventStreamConsumer {

    private final StringRedisTemplate redisTemplate;
    private final OrderEventConsumerProperties properties;
    private final OrderEventHandler orderEventHandler;
    private final IngressEventAckClient ingressEventAckClient;
    private final OrderEventProcessLogService processLogService;
    private final PelOrphanCleaner pelOrphanCleaner;

    private volatile boolean groupReady;

    public boolean isReady() {
        return groupReady;
    }

    @PostConstruct
    public void initialize() {
        ensureConsumerGroup();
    }

    /** 由订单专属 StreamMessageListenerContainer 回调，不占用定时任务线程。 */
    public void consumeNewMessage(MapRecord<String, String, String> record) {
        processOne(record, false);
    }

    public void handleStreamReadError(Throwable error) {
        if (isNoGroup(error)) {
            groupReady = false;
            log.warn("[Stream Consumer] 🔄 Order consumer group is missing; recreation is being attempted. "
                            + "stream={}, group={}",
                    properties.getStreamKey(), properties.getGroup());
            ensureConsumerGroup();
            return;
        }
        log.error("[Stream Consumer] ❌ Order Stream listener failed; the subscription remains active. stream={}",
                properties.getStreamKey(), error);
    }

    @SuppressWarnings("unchecked")
    public void reclaimPendingMessages() {
        if (!ensureConsumerGroup()) {
            return;
        }
        try {
            PendingMessages pendingMessages = redisTemplate.opsForStream().pending(
                    properties.getStreamKey(),
                    properties.getGroup(),
                    Range.unbounded(),
                    properties.getBatchSize());
            if (pendingMessages.isEmpty()) {
                return;
            }
            List<RecordId> claimIds = new ArrayList<>();
            for (PendingMessage pending : pendingMessages) {
                if (pending.getElapsedTimeSinceLastDelivery().compareTo(properties.getPendingMinIdle()) >= 0) {
                    claimIds.add(pending.getId());
                }
            }
            if (claimIds.isEmpty()) {
                return;
            }
            List<MapRecord<String, Object, Object>> claimed = redisTemplate.opsForStream().claim(
                    properties.getStreamKey(),
                    properties.getGroup(),
                    properties.getConsumerName(),
                    properties.getPendingMinIdle(),
                    claimIds.toArray(RecordId[]::new));
            List<MapRecord<String, Object, Object>> claimedRecords =
                    claimed == null ? Collections.emptyList() : claimed;
            log.info("[PEL Recovery] 🔄 Reclaiming order pending messages. requested={}, reclaimed={}, group={}",
                    claimIds.size(), claimedRecords.size(), properties.getGroup());
            process(claimedRecords, true);
            pelOrphanCleaner.clean(
                    properties.getStreamKey(), properties.getGroup(), claimIds, claimedRecords);
            log.info("[PEL Recovery] ✅ Order reclaim batch completed. requested={}, reclaimed={}, group={}",
                    claimIds.size(), claimedRecords.size(), properties.getGroup());
        } catch (Exception e) {
            if (isNoGroup(e)) {
                groupReady = false;
            }
            log.error("[PEL Recovery] ❌ Order pending-message reclaim failed; entries remain recoverable. "
                            + "stream={}, group={}",
                    properties.getStreamKey(), properties.getGroup(), e);
        }
    }

    private void process(List<? extends MapRecord<String, ?, ?>> records, boolean reclaimed) {
        for (MapRecord<String, ?, ?> record : records) {
            processOne(record, reclaimed);
        }
    }

    private void processOne(MapRecord<String, ?, ?> record, boolean reclaimed) {
        LocalDateTime startedTime = LocalDateTime.now();
        long startedNanos = System.nanoTime();
        String recordId = record.getId().getValue();
        int triggerType = reclaimed
                ? OrderEventProcessLogService.TRIGGER_PEL_RECLAIM
                : OrderEventProcessLogService.TRIGGER_STREAM;
        OrderEventMessage event;
        try {
            event = OrderEventMessage.from(record.getValue());
        } catch (Exception e) {
            recordFailureAndAcknowledge(null, record, triggerType,
                    OrderEventProcessStage.STREAM_MESSAGE, e, startedTime, startedNanos, reclaimed);
            return;
        }

        OrderPersistResult result;
        try {
            result = orderEventHandler.handle(event);
        } catch (Exception e) {
            String stage = e instanceof OrderEventProcessingException processingException
                    ? processingException.getStage() : OrderEventProcessStage.ORDER_PERSIST;
            recordFailureAndAcknowledge(event, record, triggerType,
                    stage, e, startedTime, startedNanos, reclaimed);
            return;
        }

        long processLogId;
        try {
            processLogId = processLogService.recordSuccess(
                    event, recordId, triggerType, result, startedTime, startedNanos);
        } catch (Exception e) {
            log.error("[Processing Audit] ❌ Successful order processing could not be recorded; message remains "
                            + "in PEL. "
                            + "recordId={}, eventId={}, storageId={}, eventKey={}",
                    recordId, event.eventId(), event.storageId(), event.eventKey(), e);
            return;
        }

        boolean ingressAcked = false;
        try {
            ingressEventAckClient.ack(event.contentType(), event.eventId());
            ingressAcked = true;
        } catch (Exception e) {
            recordIngressAckFailure(processLogId, e);
            log.warn("[Ingress ACK] 🔄 Order was persisted but Ingress ACK failed; Redis will be XACKed and "
                            + "the unacknowledged event will be recovered by the scheduled pull. "
                            + "recordId={}, eventId={}, storageId={}, eventKey={}",
                    recordId, event.eventId(), event.storageId(), event.eventKey(), e);
        }
        if (ingressAcked) {
            try {
                processLogService.recordIngressAck(processLogId, true);
            } catch (Exception e) {
                log.error("[Processing Audit] ❌ Successful order Ingress ACK status could not be recorded; "
                                + "the durable success audit remains authoritative. processLogId={}, recordId={}, eventId={}",
                        processLogId, recordId, event.eventId(), e);
            }
        }
        acknowledge(record, processLogId);
        log.debug("[Stream Consumer] ✅ Order event completed. recordId={}, eventId={}, storageId={}, "
                        + "eventKey={}, result={}, reclaimed={}",
                recordId, event.eventId(), event.storageId(), event.eventKey(), result, reclaimed);
    }

    private void recordFailureAndAcknowledge(
            OrderEventMessage event,
            MapRecord<String, ?, ?> record,
            int triggerType,
            String stage,
            Exception failure,
            LocalDateTime startedTime,
            long startedNanos,
            boolean reclaimed) {
        String recordId = record.getId().getValue();
        long processLogId;
        try {
            processLogId = processLogService.recordFailure(
                    event, recordId, triggerType, stage, failure, startedTime, startedNanos);
        } catch (Exception logFailure) {
            log.error("[Processing Audit] ❌ Order failure could not be recorded; message remains in PEL. "
                            + "recordId={}, eventId={}",
                    recordId, event == null ? null : event.eventId(), logFailure);
            return;
        }
        acknowledge(record, processLogId);
        log.error("[Stream Consumer] ❌ Order processing failed; Redis delivery was acknowledged and Ingress "
                        + "remains unacknowledged for recovery. "
                        + "recordId={}, eventId={}, stage={}, reclaimed={}",
                recordId, event == null ? null : event.eventId(), stage, reclaimed, failure);
    }

    private void acknowledge(MapRecord<String, ?, ?> record, long processLogId) {
        boolean succeeded = false;
        try {
            Long acknowledged = redisTemplate.opsForStream().acknowledge(
                    properties.getStreamKey(), properties.getGroup(), record.getId());
            succeeded = acknowledged != null && acknowledged > 0;
            if (!succeeded) {
                log.warn("[Redis XACK] 🔄 Order XACK affected no pending record; the record will not be deleted. "
                        + "recordId={}", record.getId().getValue());
            }
        } catch (Exception e) {
            log.error("[Redis XACK] ❌ Order XACK failed; record remains recoverable in PEL. recordId={}",
                    record.getId().getValue(), e);
        } finally {
            try {
                processLogService.recordRedisXack(processLogId, succeeded);
            } catch (Exception e) {
                log.error("[Processing Audit] ❌ Order XACK status could not be recorded. "
                                + "processLogId={}, recordId={}",
                        processLogId, record.getId().getValue(), e);
            }
        }
        if (succeeded) {
            deleteAcknowledgedRecord(record);
        }
    }

    private void deleteAcknowledgedRecord(MapRecord<String, ?, ?> record) {
        try {
            redisTemplate.opsForStream().delete(properties.getStreamKey(), record.getId());
        } catch (Exception e) {
            log.warn("[Redis Stream] 🔄 Acknowledged order record could not be deleted; MAXLEN remains the "
                            + "cleanup fallback. recordId={}",
                    record.getId().getValue(), e);
        }
    }

    private void recordIngressAckFailure(long processLogId, Exception ackFailure) {
        try {
            processLogService.recordIngressAck(processLogId, false);
        } catch (Exception statusFailure) {
            ackFailure.addSuppressed(statusFailure);
        }
    }

    private synchronized boolean ensureConsumerGroup() {
        if (groupReady) {
            return true;
        }
        RecordId initRecordId = null;
        try {
            Boolean exists = redisTemplate.hasKey(properties.getStreamKey());
            if (!Boolean.TRUE.equals(exists)) {
                initRecordId = redisTemplate.opsForStream().add(
                        properties.getStreamKey(), Map.of("init", "true"));
            }
            try {
                redisTemplate.opsForStream().createGroup(
                        properties.getStreamKey(), ReadOffset.from("0-0"), properties.getGroup());
                log.info("[Stream Consumer] ✅ Order consumer group is ready. stream={}, group={}",
                        properties.getStreamKey(), properties.getGroup());
            } catch (Exception e) {
                if (!isBusyGroup(e)) {
                    throw e;
                }
            }
            groupReady = true;
            return true;
        } catch (Exception e) {
            log.error("[Stream Consumer] ❌ Order consumer group initialization failed; consumption is paused. "
                            + "stream={}, group={}",
                    properties.getStreamKey(), properties.getGroup(), e);
            return false;
        } finally {
            if (initRecordId != null) {
                try {
                    redisTemplate.opsForStream().delete(properties.getStreamKey(), initRecordId);
                } catch (Exception e) {
                    log.warn("[Redis Stream] 🔄 Order initialization record cleanup failed; MAXLEN remains the "
                            + "fallback. recordId={}", initRecordId, e);
                }
            }
        }
    }

    private static boolean isBusyGroup(Throwable throwable) {
        return containsMessage(throwable, "BUSYGROUP");
    }

    private static boolean isNoGroup(Throwable throwable) {
        return containsMessage(throwable, "NOGROUP");
    }

    private static boolean containsMessage(Throwable throwable, String text) {
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(text)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
