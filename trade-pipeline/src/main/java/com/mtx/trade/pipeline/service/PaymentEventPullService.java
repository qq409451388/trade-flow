package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.config.EventConsumerConfiguration;
import com.mtx.trade.pipeline.config.UnackedEventPullProperties;
import com.mtx.trade.pipeline.dto.IngressUnackedEvent;
import com.mtx.trade.pipeline.dto.PaymentEventMessage;
import com.mtx.trade.pipeline.dto.PaymentEventPullCommand;
import com.mtx.trade.pipeline.dto.PaymentEventPullResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.Set;
import java.util.ArrayList;

/** Pipeline 主动拉取并处理 Ingress 未 ACK的支付事件。 */
@Slf4j
@Service
public class PaymentEventPullService {

    private static final int MAX_BATCH_SIZE = 500;

    private final IngressUnackedEventClient unackedEventClient;
    private final PaymentEventHandler paymentEventHandler;
    private final PaymentEventProcessLogService processLogService;
    private final IngressEventAckClient ingressEventAckClient;
    private final UnackedEventPullProperties unackedPullProperties;
    private final Executor unackedPullWorkerExecutor;

    public PaymentEventPullService(
            IngressUnackedEventClient unackedEventClient,
            PaymentEventHandler paymentEventHandler,
            PaymentEventProcessLogService processLogService,
            IngressEventAckClient ingressEventAckClient,
            UnackedEventPullProperties unackedPullProperties,
            @Qualifier(EventConsumerConfiguration.UNACKED_PULL_WORKER_EXECUTOR)
            Executor unackedPullWorkerExecutor) {
        this.unackedEventClient = unackedEventClient;
        this.paymentEventHandler = paymentEventHandler;
        this.processLogService = processLogService;
        this.ingressEventAckClient = ingressEventAckClient;
        this.unackedPullProperties = unackedPullProperties;
        this.unackedPullWorkerExecutor = unackedPullWorkerExecutor;
    }

    public List<PaymentEventPullResult> pull(PaymentEventPullCommand command) {
        List<Long> eventIds = validateEventIds(command == null ? null : command.eventIds());
        int limit = normalizeLimit(command == null ? null : command.limit(), eventIds);
        long afterEventId = normalizeAfterEventId(command == null ? null : command.afterEventId(), eventIds);
        List<IngressUnackedEvent> events = unackedEventClient.list(
                ContentType.PAYMENT.getCode(), eventIds, limit, afterEventId);
        Set<Long> ackOnlyEventIds = processLogService.findAckOnlyEventIds(
                events.stream().map(IngressUnackedEvent::eventId).toList(), terminalFailureAttempts());
        List<CompletableFuture<PaymentEventPullResult>> futures = events.stream()
                .filter(source -> !ackOnlyEventIds.contains(source.eventId()))
                .map(source -> CompletableFuture.supplyAsync(
                        () -> processOne(source), unackedPullWorkerExecutor))
                .toList();
        List<PaymentEventPullResult> results = new ArrayList<>();
        ackOnlyEventIds.forEach(id -> results.add(new PaymentEventPullResult(
                id, "ACK_ONLY", "success or terminal audit already exists")));
        futures.stream().map(CompletableFuture::join).forEach(results::add);
        List<Long> ackIds = results.stream().filter(result -> isAckable(result.status()))
                .map(PaymentEventPullResult::eventId).filter(java.util.Objects::nonNull).distinct().toList();
        if (!ackIds.isEmpty()) {
            try {
                ingressEventAckClient.batchAck(ContentType.PAYMENT.getCode(), ackIds);
                processLogService.recordIngressAckByEventIds(ackIds, true);
            } catch (Exception e) {
                processLogService.recordIngressAckByEventIds(ackIds, false);
                log.warn("[Ingress ACK] 🔄 Payment batch ACK failed; successful business results remain durable "
                        + "and will converge in the next sweep. eventCount={}", ackIds.size(), e);
            }
        }
        return results;
    }

    private PaymentEventPullResult processOne(IngressUnackedEvent source) {
        LocalDateTime startedTime = LocalDateTime.now();
        long startedNanos = System.nanoTime();
        PaymentEventMessage event;
        try {
            event = toEvent(source);
        } catch (Exception e) {
            processLogService.recordFailure(null, null, PaymentEventProcessLogService.TRIGGER_ACTIVE_PULL,
                    PaymentEventProcessStage.STREAM_MESSAGE, e, startedTime, startedNanos);
            return new PaymentEventPullResult(source == null ? null : source.eventId(), "FAILED", e.getMessage());
        }

        PaymentPersistResult result;
        try {
            result = paymentEventHandler.handle(event);
            processLogService.recordSuccess(
                    event, null, PaymentEventProcessLogService.TRIGGER_ACTIVE_PULL,
                    result, startedTime, startedNanos);
        } catch (Exception e) {
            String stage = e instanceof PaymentEventProcessingException processingException
                    ? processingException.getStage() : PaymentEventProcessStage.PAYMENT_PERSIST;
            try {
                processLogService.recordFailure(
                        event, null, PaymentEventProcessLogService.TRIGGER_ACTIVE_PULL,
                        stage, e, startedTime, startedNanos);
            } catch (Exception logFailure) {
                e.addSuppressed(logFailure);
                log.error("[Processing Audit] ❌ Pulled payment failure could not be recorded; Ingress remains "
                                + "unacknowledged. eventId={}, stage={}",
                        event.eventId(), stage, e);
                return new PaymentEventPullResult(event.eventId(), "AUDIT_FAILED", e.getMessage());
            }
            long attempts;
            try {
                attempts = processLogService.countActivePullFailures(event.eventId());
            } catch (Exception countFailure) {
                log.error("[Processing Audit] ❌ Active-pull failure count could not be read; Ingress remains "
                                + "unacknowledged. eventId={}",
                        event.eventId(), countFailure);
                return new PaymentEventPullResult(event.eventId(), "AUDIT_COUNT_FAILED", e.getMessage());
            }
            if (attempts < terminalFailureAttempts()) {
                log.warn("[Unacked Event Pull] 🔄 Pulled payment processing failed; Ingress remains "
                                + "unacknowledged for another active-pull attempt. eventId={}, stage={}, "
                                + "attempts={}, terminalAttempts={}",
                        event.eventId(), stage, attempts, terminalFailureAttempts(), e);
                return new PaymentEventPullResult(event.eventId(), "RETRYABLE_FAILED",
                        failureMessage(stage, attempts, e));
            }
            log.error("[Unacked Event Pull] ❌ Payment reached terminal failure; durable audit permits batch ACK. "
                    + "eventId={}, stage={}, attempts={}", event.eventId(), stage, attempts, e);
            return new PaymentEventPullResult(event.eventId(), "TERMINAL_FAILED",
                    failureMessage(stage, attempts, e));
        }
        return new PaymentEventPullResult(event.eventId(), result.name(), "processed");
    }

    private static boolean isAckable(String status) {
        return "ACK_ONLY".equals(status) || "APPLIED".equals(status) || "IGNORED".equals(status)
                || "TERMINAL_FAILED".equals(status);
    }

    private int terminalFailureAttempts() {
        int value = unackedPullProperties.getTerminalFailureAttempts();
        if (value <= 0 || value > 100) {
            throw new IllegalArgumentException(
                    "trade.pipeline.unacked-event-pull.terminal-failure-attempts 必须为1~100");
        }
        return value;
    }

    private static String failureMessage(String stage, long attempts, Exception failure) {
        return "stage=" + stage + ", attempts=" + attempts + ", reason=" + failure.getMessage();
    }

    private static PaymentEventMessage toEvent(IngressUnackedEvent source) {
        if (source == null || source.eventId() == null || source.storageId() == null
                || source.sourceSystem() == null || source.contentType() == null
                || source.messageVersion() == null || source.thirdEventKey() == null
                || source.storageSha256() == null) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "Ingress未ACK支付事件字段不完整");
        }
        byte[] sha;
        try {
            sha = HexFormat.of().parseHex(source.storageSha256());
        } catch (IllegalArgumentException e) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "Ingress未ACK支付事件storageSha256无效");
        }
        return new PaymentEventMessage(source.eventId(), source.storageId(), sha,
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
        if (!eventIds.isEmpty()) {
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
