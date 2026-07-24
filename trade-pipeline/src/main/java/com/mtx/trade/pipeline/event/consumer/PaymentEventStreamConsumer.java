package com.mtx.trade.pipeline.event.consumer;

import com.mtx.trade.pipeline.config.PaymentEventConsumerProperties;
import com.mtx.trade.pipeline.dto.PaymentEventMessage;
import com.mtx.trade.pipeline.enums.PaymentEventProcessStage;
import com.mtx.trade.pipeline.enums.PaymentPersistResult;
import com.mtx.trade.pipeline.event.processor.PaymentEventHandler;
import com.mtx.trade.pipeline.exception.PaymentEventProcessingException;
import com.mtx.trade.pipeline.service.*;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.mtx.trade.pipeline.config.EventConsumerConfiguration.PAYMENT_EVENT_WORKER_EXECUTOR;

/** 支付事件消费、PEL 接管、处理审计及双重 ACK 编排。 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "trade.pipeline.payment-event-consumer",
        name = "enabled", havingValue = "true", matchIfMissing = true)
public class PaymentEventStreamConsumer {

    private final StringRedisTemplate redisTemplate;
    private final PaymentEventConsumerProperties properties;
    private final PaymentEventHandler paymentEventHandler;
    private final IngressEventAckClient ingressEventAckClient;
    private final PaymentEventProcessLogService processLogService;
    private final PelOrphanCleaner pelOrphanCleaner;
    private final PartitionedEventExecutor workerExecutor;
    private final EventProcessingTelemetry telemetry;

    private volatile boolean groupReady;

    public PaymentEventStreamConsumer(
            StringRedisTemplate redisTemplate,
            PaymentEventConsumerProperties properties,
            PaymentEventHandler paymentEventHandler,
            IngressEventAckClient ingressEventAckClient,
            PaymentEventProcessLogService processLogService,
            PelOrphanCleaner pelOrphanCleaner,
            @Qualifier(PAYMENT_EVENT_WORKER_EXECUTOR) PartitionedEventExecutor workerExecutor,
            EventProcessingTelemetry telemetry) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.paymentEventHandler = paymentEventHandler;
        this.ingressEventAckClient = ingressEventAckClient;
        this.processLogService = processLogService;
        this.pelOrphanCleaner = pelOrphanCleaner;
        this.workerExecutor = workerExecutor;
        this.telemetry = telemetry;
    }

    public boolean isReady() {
        return groupReady;
    }

    @PostConstruct
    public void initialize() {
        ensureConsumerGroup();
    }

    /** 由支付专属 StreamMessageListenerContainer 回调，不占用定时任务线程。 */
    public void consumeNewMessage(MapRecord<String, String, String> record) {
        dispatch(record, false);
    }

    public void handleStreamReadError(Throwable error) {
        if (isNoGroup(error)) {
            groupReady = false;
            log.warn("[Stream Consumer] 🔄 Payment consumer group is missing; recreation is being attempted. "
                            + "stream={}, group={}",
                    properties.getStreamKey(), properties.getGroup());
            ensureConsumerGroup();
            return;
        }
        log.error("[Stream Consumer] ❌ Payment Stream listener failed; the subscription remains active. stream={}",
                properties.getStreamKey(), error);
    }

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

    private void process(List<? extends MapRecord<String, ?, ?>> records, boolean reclaimed) {
        CompletableFuture<?>[] completions = records.stream()
                .map(record -> dispatch(record, reclaimed))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(completions).join();
    }

    private CompletableFuture<Void> dispatch(MapRecord<String, ?, ?> record, boolean reclaimed) {
        return workerExecutor.submit(partitionKey(record), () -> processOne(record, reclaimed))
                .whenComplete((ignored, failure) -> {
                    if (failure != null) {
                        telemetry.markFailure(EventProcessingTelemetry.PAYMENT);
                        log.error("[Stream Consumer] ❌ Payment worker terminated unexpectedly; the record remains "
                                        + "recoverable in PEL. recordId={}",
                                record.getId().getValue(), failure);
                    }
                });
    }

    private static String partitionKey(MapRecord<String, ?, ?> record) {
        Object sourceSystem = record.getValue().get("sourceSystem");
        Object eventKey = record.getValue().get("eventKey");
        if (sourceSystem != null && eventKey != null && !eventKey.toString().isBlank()) {
            return sourceSystem + ":" + eventKey;
        }
        return "invalid:" + record.getId().getValue();
    }

    private void processOne(MapRecord<String, ?, ?> record, boolean reclaimed) {
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
        telemetry.markSuccess(EventProcessingTelemetry.PAYMENT);
        boolean ingressAcked = false;
        try {
            ingressEventAckClient.ack(event.contentType(), event.eventId());
            ingressAcked = true;
        } catch (Exception e) {
            telemetry.markIngressAckFailure(EventProcessingTelemetry.PAYMENT);
            recordIngressAckFailure(processLogId, e);
            log.warn("[Ingress ACK] 🔄 Payment was persisted but Ingress ACK failed; Redis will be XACKed and "
                            + "the unacknowledged event will be recovered by the scheduled pull. "
                            + "recordId={}, eventId={}, storageId={}, eventKey={}",
                    recordId, event.eventId(), event.storageId(), event.eventKey(), e);
        }
        if (ingressAcked) {
            try {
                processLogService.recordIngressAck(processLogId, true);
            } catch (Exception e) {
                log.error("[Processing Audit] ❌ Successful payment Ingress ACK status could not be recorded; "
                                + "the durable success audit remains authoritative. processLogId={}, recordId={}, eventId={}",
                        processLogId, recordId, event.eventId(), e);
            }
        }
        acknowledge(record, processLogId);
        log.debug("[Stream Consumer] ✅ Payment event completed. recordId={}, eventId={}, storageId={}, "
                        + "eventKey={}, result={}, reclaimed={}",
                recordId, event.eventId(), event.storageId(), event.eventKey(), result, reclaimed);
    }

    private void recordFailureAndAcknowledge(
            PaymentEventMessage event,
            MapRecord<String, ?, ?> record,
            int triggerType,
            String stage,
            Exception failure,
            LocalDateTime startedTime,
            long startedNanos,
            boolean reclaimed) {
        telemetry.markFailure(EventProcessingTelemetry.PAYMENT);
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

    private void acknowledge(MapRecord<String, ?, ?> record, long processLogId) {
        boolean succeeded = false;
        try {
            Long acknowledged = redisTemplate.opsForStream().acknowledge(
                    properties.getStreamKey(), properties.getGroup(), record.getId());
            if (acknowledged != null && acknowledged > 0) {
                succeeded = true;
            } else {
                // XACK returns 0 when the record is no longer in PEL — it was already
                // acknowledged by a concurrent thread (stream listener vs PEL reclaim race).
                // The desired end state is achieved; treat as success and clean up the record.
                succeeded = true;
                log.debug("[Redis XACK] Payment record was already acknowledged by a concurrent thread; "
                        + "stream cleanup will proceed. recordId={}", record.getId().getValue());
            }
        } catch (Exception e) {
            log.error("[Redis XACK] ❌ Payment XACK failed; record remains recoverable in PEL. recordId={}",
                    record.getId().getValue(), e);
        } finally {
            if (!succeeded) {
                telemetry.markRedisXackFailure(EventProcessingTelemetry.PAYMENT);
            }
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

    private void deleteAcknowledgedRecord(MapRecord<String, ?, ?> record) {
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
