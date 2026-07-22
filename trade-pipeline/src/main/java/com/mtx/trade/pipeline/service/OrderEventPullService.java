package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.dto.IngressExhaustedEvent;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.pipeline.config.EventConsumerConfiguration;
import com.mtx.trade.pipeline.config.ExhaustedEventPullProperties;
import com.mtx.trade.pipeline.dto.OrderEventMessage;
import com.mtx.trade.pipeline.dto.OrderEventPullCommand;
import com.mtx.trade.pipeline.dto.OrderEventPullResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Pipeline 主动拉取并直接处理 Ingress 投递耗尽事件，不经过 Redis。 */
@Slf4j
@Service
public class OrderEventPullService {

    private static final int MAX_BATCH_SIZE = 500;

    private final IngressExhaustedEventClient exhaustedEventClient;
    private final OrderEventHandler orderEventHandler;
    private final OrderEventProcessLogService processLogService;
    private final IngressEventAckClient ingressEventAckClient;
    private final ExhaustedEventPullProperties exhaustedPullProperties;
    private final Executor exhaustedPullWorkerExecutor;

    public OrderEventPullService(
            IngressExhaustedEventClient exhaustedEventClient,
            OrderEventHandler orderEventHandler,
            OrderEventProcessLogService processLogService,
            IngressEventAckClient ingressEventAckClient,
            ExhaustedEventPullProperties exhaustedPullProperties,
            @Qualifier(EventConsumerConfiguration.EXHAUSTED_PULL_WORKER_EXECUTOR)
            Executor exhaustedPullWorkerExecutor) {
        this.exhaustedEventClient = exhaustedEventClient;
        this.orderEventHandler = orderEventHandler;
        this.processLogService = processLogService;
        this.ingressEventAckClient = ingressEventAckClient;
        this.exhaustedPullProperties = exhaustedPullProperties;
        this.exhaustedPullWorkerExecutor = exhaustedPullWorkerExecutor;
    }

    public List<OrderEventPullResult> pull(OrderEventPullCommand command) {
        List<Long> eventIds = validateEventIds(command == null ? null : command.eventIds());
        int limit = normalizeLimit(command == null ? null : command.limit(), eventIds);
        long afterEventId = normalizeAfterEventId(command == null ? null : command.afterEventId(), eventIds);
        List<IngressExhaustedEvent> events = exhaustedEventClient.list(
                ContentType.ORDER.getCode(), eventIds, limit, afterEventId);
        List<CompletableFuture<OrderEventPullResult>> futures = events.stream()
                .map(source -> CompletableFuture.supplyAsync(
                        () -> processOne(source), exhaustedPullWorkerExecutor))
                .toList();
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private OrderEventPullResult processOne(IngressExhaustedEvent source) {
        LocalDateTime startedTime = LocalDateTime.now();
        long startedNanos = System.nanoTime();
        OrderEventMessage event;
        try {
            event = toEvent(source);
        } catch (Exception e) {
            processLogService.recordFailure(null, null, OrderEventProcessLogService.TRIGGER_ACTIVE_PULL,
                    OrderEventProcessStage.STREAM_MESSAGE, e, startedTime, startedNanos);
            return new OrderEventPullResult(source == null ? null : source.eventId(), "FAILED", e.getMessage());
        }

        OrderPersistResult result;
        long processLogId;
        try {
            result = orderEventHandler.handle(event);
            processLogId = processLogService.recordSuccess(
                    event, null, OrderEventProcessLogService.TRIGGER_ACTIVE_PULL,
                    result, startedTime, startedNanos);
        } catch (Exception e) {
            String stage = e instanceof OrderEventProcessingException processingException
                    ? processingException.getStage() : OrderEventProcessStage.ORDER_PERSIST;
            long failureLogId;
            try {
                failureLogId = processLogService.recordFailure(
                        event, null, OrderEventProcessLogService.TRIGGER_ACTIVE_PULL,
                        stage, e, startedTime, startedNanos);
            } catch (Exception logFailure) {
                e.addSuppressed(logFailure);
                log.error("[Processing Audit] ❌ Pulled order failure could not be recorded; Ingress remains "
                                + "unacknowledged. eventId={}, stage={}",
                        event.eventId(), stage, e);
                return new OrderEventPullResult(event.eventId(), "AUDIT_FAILED", e.getMessage());
            }
            long attempts;
            try {
                attempts = processLogService.countActivePullFailures(event.eventId());
            } catch (Exception countFailure) {
                log.error("[Processing Audit] ❌ Active-pull failure count could not be read; Ingress remains "
                                + "unacknowledged. eventId={}",
                        event.eventId(), countFailure);
                return new OrderEventPullResult(event.eventId(), "AUDIT_COUNT_FAILED", e.getMessage());
            }
            if (attempts < terminalFailureAttempts()) {
                log.warn("[Exhausted Event Pull] 🔄 Pulled order processing failed; Ingress remains "
                                + "unacknowledged for another active-pull attempt. eventId={}, stage={}, "
                                + "attempts={}, terminalAttempts={}",
                        event.eventId(), stage, attempts, terminalFailureAttempts(), e);
                return new OrderEventPullResult(event.eventId(), "RETRYABLE_FAILED",
                        failureMessage(stage, attempts, e));
            }
            return acknowledgeTerminalFailure(event, failureLogId, stage, attempts, e);
        }
        try {
            ingressEventAckClient.ack(event.contentType(), event.eventId());
        } catch (Exception e) {
            try {
                processLogService.recordIngressAck(processLogId, false);
            } catch (Exception statusFailure) {
                e.addSuppressed(statusFailure);
            }
            log.error("[Ingress ACK] ❌ Pulled order was persisted but Ingress ACK failed. eventId={}",
                    event.eventId(), e);
            return new OrderEventPullResult(event.eventId(), "ACK_FAILED", e.getMessage());
        }
        try {
            processLogService.recordIngressAck(processLogId, true);
        } catch (Exception e) {
            log.error("[Processing Audit] ❌ Pulled order ACK succeeded but audit update failed. "
                            + "eventId={}, processLogId={}",
                    event.eventId(), processLogId, e);
            return new OrderEventPullResult(event.eventId(), "ACK_AUDIT_FAILED", e.getMessage());
        }
        return new OrderEventPullResult(event.eventId(), result.name(), "processed");
    }

    private OrderEventPullResult acknowledgeTerminalFailure(
            OrderEventMessage event, long processLogId, String stage, long attempts, Exception failure) {
        try {
            ingressEventAckClient.ack(event.contentType(), event.eventId());
        } catch (Exception ackFailure) {
            try {
                processLogService.recordIngressAck(processLogId, false);
            } catch (Exception auditFailure) {
                ackFailure.addSuppressed(auditFailure);
            }
            log.error("[Ingress ACK] ❌ Terminal order failure was audited but Ingress ACK failed; the event "
                            + "remains retryable. eventId={}, stage={}, attempts={}",
                    event.eventId(), stage, attempts, ackFailure);
            return new OrderEventPullResult(event.eventId(), "ACK_FAILED",
                    failureMessage(stage, attempts, failure));
        }
        try {
            processLogService.recordIngressAck(processLogId, true);
        } catch (Exception auditFailure) {
            log.error("[Processing Audit] ❌ Terminal order failure was ACKed but the audit ACK status update "
                            + "failed. eventId={}, processLogId={}",
                    event.eventId(), processLogId, auditFailure);
            return new OrderEventPullResult(event.eventId(), "TERMINAL_ACK_AUDIT_FAILED",
                    failureMessage(stage, attempts, failure));
        }
        log.error("[Exhausted Event Pull] ❌ Order reached terminal failure; failure audit is durable and "
                        + "Ingress was acknowledged. eventId={}, stage={}, attempts={}",
                event.eventId(), stage, attempts, failure);
        return new OrderEventPullResult(event.eventId(), "TERMINAL_FAILED",
                failureMessage(stage, attempts, failure));
    }

    private int terminalFailureAttempts() {
        int value = exhaustedPullProperties.getTerminalFailureAttempts();
        if (value <= 0 || value > 100) {
            throw new IllegalArgumentException(
                    "trade.pipeline.exhausted-event-pull.terminal-failure-attempts 必须为1~100");
        }
        return value;
    }

    private static String failureMessage(String stage, long attempts, Exception failure) {
        return "stage=" + stage + ", attempts=" + attempts + ", reason=" + failure.getMessage();
    }

    private static OrderEventMessage toEvent(IngressExhaustedEvent source) {
        if (source == null || source.eventId() == null || source.storageId() == null
                || source.sourceSystem() == null || source.contentType() == null
                || source.messageVersion() == null || source.thirdEventKey() == null
                || source.storageSha256() == null) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "Ingress耗尽事件字段不完整");
        }
        byte[] sha;
        try {
            sha = HexFormat.of().parseHex(source.storageSha256());
        } catch (IllegalArgumentException e) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "Ingress耗尽事件storageSha256无效");
        }
        return new OrderEventMessage(source.eventId(), source.storageId(), sha,
                source.thirdEventKey(), source.sourceSystem(), source.contentType(), source.messageVersion());
    }

    private static List<Long> validateEventIds(List<Long> eventIds) {
        if (eventIds == null || eventIds.isEmpty()) {
            return List.of();
        }
        List<Long> normalized = eventIds.stream().distinct().toList();
        if (normalized.size() > MAX_BATCH_SIZE
                || normalized.stream().anyMatch(id -> id == null || id <= 0)) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "eventIds无效或超过500条");
        }
        return normalized;
    }

    private static int normalizeLimit(Integer requestedLimit, List<Long> eventIds) {
        if (eventIds != null && !eventIds.isEmpty()) {
            return eventIds.size();
        }
        int limit = requestedLimit == null ? 100 : requestedLimit;
        if (limit <= 0 || limit > MAX_BATCH_SIZE) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "limit必须在1至500之间");
        }
        return limit;
    }

    private static long normalizeAfterEventId(Long afterEventId, List<Long> eventIds) {
        long normalized = afterEventId == null ? 0L : afterEventId;
        if (normalized < 0 || (normalized > 0 && eventIds != null && !eventIds.isEmpty())) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "afterEventId无效或不能与eventIds同时使用");
        }
        return normalized;
    }
}
