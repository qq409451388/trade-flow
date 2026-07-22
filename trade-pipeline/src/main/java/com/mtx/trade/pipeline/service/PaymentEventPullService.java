package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.dto.IngressExhaustedEvent;
import com.mtx.trade.pipeline.dto.PaymentEventMessage;
import com.mtx.trade.pipeline.dto.PaymentEventPullCommand;
import com.mtx.trade.pipeline.dto.PaymentEventPullResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/** Pipeline 主动拉取并处理 Ingress 投递耗尽的支付事件。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentEventPullService {

    private static final int MAX_BATCH_SIZE = 500;

    private final IngressExhaustedEventClient exhaustedEventClient;
    private final PaymentEventHandler paymentEventHandler;
    private final PaymentEventProcessLogService processLogService;
    private final IngressEventAckClient ingressEventAckClient;

    public List<PaymentEventPullResult> pull(PaymentEventPullCommand command) {
        List<Long> eventIds = validateEventIds(command == null ? null : command.eventIds());
        int limit = normalizeLimit(command == null ? null : command.limit(), eventIds);
        List<IngressExhaustedEvent> events = exhaustedEventClient.list(
                ContentType.PAYMENT.getCode(), eventIds, limit);
        List<PaymentEventPullResult> results = new ArrayList<>(events.size());
        for (IngressExhaustedEvent source : events) {
            results.add(processOne(source));
        }
        return results;
    }

    private PaymentEventPullResult processOne(IngressExhaustedEvent source) {
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
        long processLogId;
        try {
            result = paymentEventHandler.handle(event);
            processLogId = processLogService.recordSuccess(
                    event, null, PaymentEventProcessLogService.TRIGGER_ACTIVE_PULL,
                    result, startedTime, startedNanos);
        } catch (Exception e) {
            String stage = e instanceof PaymentEventProcessingException processingException
                    ? processingException.getStage() : PaymentEventProcessStage.PAYMENT_PERSIST;
            try {
                processLogService.recordFailure(event, null, PaymentEventProcessLogService.TRIGGER_ACTIVE_PULL,
                        stage, e, startedTime, startedNanos);
            } catch (Exception logFailure) {
                e.addSuppressed(logFailure);
            }
            log.error("actively pulled payment event processing failed, eventId={}, stage={}",
                    event.eventId(), stage, e);
            return new PaymentEventPullResult(event.eventId(), "FAILED", e.getMessage());
        }
        try {
            ingressEventAckClient.ack(event.contentType(), event.eventId());
        } catch (Exception e) {
            try {
                processLogService.recordIngressAck(processLogId, false);
            } catch (Exception statusFailure) {
                e.addSuppressed(statusFailure);
            }
            log.error("actively pulled payment persisted but Ingress ACK failed, eventId={}", event.eventId(), e);
            return new PaymentEventPullResult(event.eventId(), "ACK_FAILED", e.getMessage());
        }
        try {
            processLogService.recordIngressAck(processLogId, true);
        } catch (Exception e) {
            log.error("actively pulled payment event ACK succeeded but audit update failed, "
                            + "eventId={}, processLogId={}",
                    event.eventId(), processLogId, e);
            return new PaymentEventPullResult(event.eventId(), "ACK_AUDIT_FAILED", e.getMessage());
        }
        return new PaymentEventPullResult(event.eventId(), result.name(), "processed");
    }

    private static PaymentEventMessage toEvent(IngressExhaustedEvent source) {
        if (source == null || source.eventId() == null || source.storageId() == null
                || source.sourceSystem() == null || source.contentType() == null
                || source.messageVersion() == null || source.thirdEventKey() == null
                || source.storageSha256() == null) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "Ingress耗尽支付事件字段不完整");
        }
        byte[] sha;
        try {
            sha = HexFormat.of().parseHex(source.storageSha256());
        } catch (IllegalArgumentException e) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "Ingress耗尽支付事件storageSha256无效");
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
}
