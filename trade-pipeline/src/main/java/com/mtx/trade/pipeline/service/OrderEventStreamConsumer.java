package com.mtx.trade.pipeline.service;

import com.mtx.trade.pipeline.config.OrderEventConsumerProperties;
import com.mtx.trade.pipeline.dto.OrderEventMessage;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
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

    @Scheduled(fixedDelayString = "${trade.pipeline.order-event-consumer.poll-delay:1000}")
    @SuppressWarnings("unchecked")
    public void consumeNewMessages() {
        if (!ensureConsumerGroup()) {
            return;
        }
        try {
            Consumer consumer = Consumer.from(properties.getGroup(), properties.getConsumerName());
            StreamOffset<String> offset = StreamOffset.create(
                    properties.getStreamKey(), ReadOffset.lastConsumed());
            List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                    consumer,
                    StreamReadOptions.empty()
                            .count(properties.getBatchSize())
                            .block(properties.getBlockTimeout()),
                    offset);
            process(records == null ? Collections.emptyList() : records, false);
        } catch (Exception e) {
            if (isNoGroup(e)) {
                groupReady = false;
            }
            log.error("read order event stream failed, stream={}", properties.getStreamKey(), e);
        }
    }

    @Scheduled(fixedDelayString = "${trade.pipeline.order-event-consumer.reclaim-delay:30000}")
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
            process(claimedRecords, true);
            pelOrphanCleaner.clean(
                    properties.getStreamKey(), properties.getGroup(), claimIds, claimedRecords);
        } catch (Exception e) {
            if (isNoGroup(e)) {
                groupReady = false;
            }
            log.error("reclaim order event PEL failed, stream={}, group={}",
                    properties.getStreamKey(), properties.getGroup(), e);
        }
    }

    private void process(List<MapRecord<String, Object, Object>> records, boolean reclaimed) {
        for (MapRecord<String, Object, Object> record : records) {
            processOne(record, reclaimed);
        }
    }

    private void processOne(MapRecord<String, Object, Object> record, boolean reclaimed) {
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
            log.error("record successful order event attempt failed; message remains pending, "
                            + "recordId={}, eventId={}, storageId={}, eventKey={}",
                    recordId, event.eventId(), event.storageId(), event.eventKey(), e);
            return;
        }

        try {
            ingressEventAckClient.ack(event.contentType(), event.eventId());
        } catch (Exception e) {
            recordIngressAckFailure(processLogId, e);
            log.error("order persisted but Ingress ACK failed; message remains pending, "
                            + "recordId={}, eventId={}, storageId={}, eventKey={}",
                    recordId, event.eventId(), event.storageId(), event.eventKey(), e);
            return;
        }
        try {
            processLogService.recordIngressAck(processLogId, true);
        } catch (Exception e) {
            log.error("record successful order Ingress ACK failed; message remains pending, "
                            + "processLogId={}, recordId={}, eventId={}",
                    processLogId, recordId, event.eventId(), e);
            return;
        }
        acknowledge(record, processLogId);
        log.info("order event processed, recordId={}, eventId={}, storageId={}, eventKey={}, result={}, reclaimed={}",
                recordId, event.eventId(), event.storageId(), event.eventKey(), result, reclaimed);
    }

    private void recordFailureAndAcknowledge(
            OrderEventMessage event,
            MapRecord<String, Object, Object> record,
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
            log.error("record failed order event attempt failed; message remains pending, recordId={}, eventId={}",
                    recordId, event == null ? null : event.eventId(), logFailure);
            return;
        }
        acknowledge(record, processLogId);
        log.error("order event processing failed and Redis delivery was acknowledged; Ingress remains unacked, "
                        + "recordId={}, eventId={}, stage={}, reclaimed={}",
                recordId, event == null ? null : event.eventId(), stage, reclaimed, failure);
    }

    private void acknowledge(MapRecord<String, Object, Object> record, long processLogId) {
        boolean succeeded = false;
        try {
            redisTemplate.opsForStream().acknowledge(
                    properties.getStreamKey(), properties.getGroup(), record.getId());
            succeeded = true;
        } catch (Exception e) {
            log.error("Redis XACK failed; record remains recoverable in PEL, recordId={}",
                    record.getId().getValue(), e);
        } finally {
            try {
                processLogService.recordRedisXack(processLogId, succeeded);
            } catch (Exception e) {
                log.error("record order Redis XACK status failed, processLogId={}, recordId={}",
                        processLogId, record.getId().getValue(), e);
            }
        }
        if (succeeded) {
            deleteAcknowledgedRecord(record);
        }
    }

    private void deleteAcknowledgedRecord(MapRecord<String, Object, Object> record) {
        try {
            redisTemplate.opsForStream().delete(properties.getStreamKey(), record.getId());
        } catch (Exception e) {
            log.warn("delete acknowledged order Stream record failed; MAXLEN remains as fallback, recordId={}",
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
                log.info("order event consumer group created, stream={}, group={}",
                        properties.getStreamKey(), properties.getGroup());
            } catch (Exception e) {
                if (!isBusyGroup(e)) {
                    throw e;
                }
            }
            groupReady = true;
            return true;
        } catch (Exception e) {
            log.error("initialize order event consumer group failed, stream={}, group={}",
                    properties.getStreamKey(), properties.getGroup(), e);
            return false;
        } finally {
            if (initRecordId != null) {
                try {
                    redisTemplate.opsForStream().delete(properties.getStreamKey(), initRecordId);
                } catch (Exception e) {
                    log.warn("delete order stream init record failed, recordId={}", initRecordId, e);
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
