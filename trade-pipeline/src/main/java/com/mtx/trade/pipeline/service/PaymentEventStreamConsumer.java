package com.mtx.trade.pipeline.service;

import com.mtx.trade.pipeline.config.PaymentEventConsumerProperties;
import com.mtx.trade.pipeline.dto.PaymentEventMessage;
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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** 支付事件消费、PEL 接管、处理审计及双重 ACK 编排。 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "trade.pipeline.payment-event-consumer",
        name = "enabled", havingValue = "true", matchIfMissing = true)
public class PaymentEventStreamConsumer {

    private final StringRedisTemplate redisTemplate;
    private final PaymentEventConsumerProperties properties;
    private final PaymentEventHandler paymentEventHandler;
    private final IngressEventAckClient ingressEventAckClient;
    private final PaymentEventProcessLogService processLogService;
    private final PelOrphanCleaner pelOrphanCleaner;

    private volatile boolean groupReady;

    public boolean isReady() {
        return groupReady;
    }

    @PostConstruct
    public void initialize() {
        ensureConsumerGroup();
    }

    @Scheduled(fixedDelayString = "${trade.pipeline.payment-event-consumer.poll-delay:1000}")
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
                    StreamReadOptions.empty().count(properties.getBatchSize()).block(properties.getBlockTimeout()),
                    offset);
            process(records == null ? Collections.emptyList() : records, false);
        } catch (Exception e) {
            if (isNoGroup(e)) {
                groupReady = false;
            }
            log.error("[Stream Consumer] ❌ Payment Stream read failed; polling will retry. stream={}",
                    properties.getStreamKey(), e);
        }
    }

    @Scheduled(fixedDelayString = "${trade.pipeline.payment-event-consumer.reclaim-delay:30000}")
    @SuppressWarnings("unchecked")
    public void reclaimPendingMessages() {
        if (!ensureConsumerGroup()) {
            return;
        }
        try {
            PendingMessages pendingMessages = redisTemplate.opsForStream().pending(
                    properties.getStreamKey(), properties.getGroup(), Range.unbounded(), properties.getBatchSize());
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
                    properties.getStreamKey(), properties.getGroup(), properties.getConsumerName(),
                    properties.getPendingMinIdle(), claimIds.toArray(RecordId[]::new));
            List<MapRecord<String, Object, Object>> claimedRecords =
                    claimed == null ? Collections.emptyList() : claimed;
            log.info("[PEL Recovery] 🔄 Reclaiming payment pending messages. requested={}, reclaimed={}, group={}",
                    claimIds.size(), claimedRecords.size(), properties.getGroup());
            process(claimedRecords, true);
            pelOrphanCleaner.clean(
                    properties.getStreamKey(), properties.getGroup(), claimIds, claimedRecords);
            log.info("[PEL Recovery] ✅ Payment reclaim batch completed. requested={}, reclaimed={}, group={}",
                    claimIds.size(), claimedRecords.size(), properties.getGroup());
        } catch (Exception e) {
            if (isNoGroup(e)) {
                groupReady = false;
            }
            log.error("[PEL Recovery] ❌ Payment pending-message reclaim failed; entries remain recoverable. "
                            + "stream={}, group={}",
                    properties.getStreamKey(), properties.getGroup(), e);
        }
    }

    private void process(List<MapRecord<String, Object, Object>> records, boolean reclaimed) {
        records.forEach(record -> processOne(record, reclaimed));
    }

    private void processOne(MapRecord<String, Object, Object> record, boolean reclaimed) {
        LocalDateTime startedTime = LocalDateTime.now();
        long startedNanos = System.nanoTime();
        String recordId = record.getId().getValue();
        int triggerType = reclaimed
                ? PaymentEventProcessLogService.TRIGGER_PEL_RECLAIM
                : PaymentEventProcessLogService.TRIGGER_STREAM;
        PaymentEventMessage event;
        try {
            event = PaymentEventMessage.from(record.getValue());
        } catch (Exception e) {
            recordFailureAndAcknowledge(null, record, triggerType,
                    PaymentEventProcessStage.STREAM_MESSAGE, e, startedTime, startedNanos, reclaimed);
            return;
        }

        PaymentPersistResult result;
        try {
            result = paymentEventHandler.handle(event);
        } catch (Exception e) {
            String stage = e instanceof PaymentEventProcessingException processingException
                    ? processingException.getStage() : PaymentEventProcessStage.PAYMENT_PERSIST;
            recordFailureAndAcknowledge(
                    event, record, triggerType, stage, e, startedTime, startedNanos, reclaimed);
            return;
        }

        long processLogId;
        try {
            processLogId = processLogService.recordSuccess(
                    event, recordId, triggerType, result, startedTime, startedNanos);
        } catch (Exception e) {
            log.error("[Processing Audit] ❌ Successful payment processing could not be recorded; message remains "
                            + "in PEL. "
                            + "recordId={}, eventId={}, storageId={}, eventKey={}",
                    recordId, event.eventId(), event.storageId(), event.eventKey(), e);
            return;
        }
        try {
            ingressEventAckClient.ack(event.contentType(), event.eventId());
        } catch (Exception e) {
            recordIngressAckFailure(processLogId, e);
            log.error("[Ingress ACK] ❌ Payment was persisted but Ingress ACK failed; message remains in PEL. "
                            + "recordId={}, eventId={}, storageId={}, eventKey={}",
                    recordId, event.eventId(), event.storageId(), event.eventKey(), e);
            return;
        }
        try {
            processLogService.recordIngressAck(processLogId, true);
        } catch (Exception e) {
            log.error("[Processing Audit] ❌ Successful payment Ingress ACK could not be recorded; message remains "
                            + "in PEL. "
                            + "processLogId={}, recordId={}, eventId={}",
                    processLogId, recordId, event.eventId(), e);
            return;
        }
        acknowledge(record, processLogId);
        log.debug("[Stream Consumer] ✅ Payment event completed. recordId={}, eventId={}, storageId={}, "
                        + "eventKey={}, result={}, reclaimed={}",
                recordId, event.eventId(), event.storageId(), event.eventKey(), result, reclaimed);
    }

    private void recordFailureAndAcknowledge(
            PaymentEventMessage event,
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
            log.error("[Processing Audit] ❌ Payment failure could not be recorded; message remains in PEL. "
                            + "recordId={}, eventId={}",
                    recordId, event == null ? null : event.eventId(), logFailure);
            return;
        }
        acknowledge(record, processLogId);
        log.error("[Stream Consumer] ❌ Payment processing failed; Redis delivery was acknowledged and Ingress "
                        + "remains unacknowledged for recovery. "
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
            log.error("[Redis XACK] ❌ Payment XACK failed; record remains recoverable in PEL. recordId={}",
                    record.getId().getValue(), e);
        } finally {
            try {
                processLogService.recordRedisXack(processLogId, succeeded);
            } catch (Exception e) {
                log.error("[Processing Audit] ❌ Payment XACK status could not be recorded. "
                                + "processLogId={}, recordId={}",
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
            log.warn("[Redis Stream] 🔄 Acknowledged payment record could not be deleted; MAXLEN remains the "
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
            if (!Boolean.TRUE.equals(redisTemplate.hasKey(properties.getStreamKey()))) {
                initRecordId = redisTemplate.opsForStream().add(properties.getStreamKey(), Map.of("init", "true"));
            }
            try {
                redisTemplate.opsForStream().createGroup(
                        properties.getStreamKey(), ReadOffset.from("0-0"), properties.getGroup());
                log.info("[Stream Consumer] ✅ Payment consumer group is ready. stream={}, group={}",
                        properties.getStreamKey(), properties.getGroup());
            } catch (Exception e) {
                if (!isBusyGroup(e)) {
                    throw e;
                }
            }
            groupReady = true;
            return true;
        } catch (Exception e) {
            log.error("[Stream Consumer] ❌ Payment consumer group initialization failed; consumption is paused. "
                            + "stream={}, group={}",
                    properties.getStreamKey(), properties.getGroup(), e);
            return false;
        } finally {
            if (initRecordId != null) {
                try {
                    redisTemplate.opsForStream().delete(properties.getStreamKey(), initRecordId);
                } catch (Exception e) {
                    log.warn("[Redis Stream] 🔄 Payment initialization record cleanup failed; MAXLEN remains the "
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
