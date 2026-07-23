package com.mtx.trade.ingress.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.common.enums.EventAckStatus;
import com.mtx.trade.ingress.config.EventDeliveryConfiguration;
import com.mtx.trade.ingress.config.EventDeliveryProperties;
import com.mtx.trade.ingress.dto.EventDeliveryVO;
import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.entity.PaymentEventDO;
import com.mtx.trade.ingress.service.db.OrderEventDbService;
import com.mtx.trade.ingress.service.db.PaymentEventDbService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.function.IntConsumer;

/** Redis 实时通知和 Pipeline 未 ACK 事件查询。MySQL event 始终是可靠事实源。 */
@Slf4j
@Service
public class EventDeliveryService {

    private static final int MAX_QUERY_SIZE = 500;

    private final EventStreamPublisher publisher;
    private final OrderEventDbService orderEventDbService;
    private final PaymentEventDbService paymentEventDbService;
    private final TaskScheduler retryScheduler;
    private final EventDeliveryProperties properties;
    private final EventDeliveryCircuitBreaker circuitBreaker;

    public EventDeliveryService(
            EventStreamPublisher publisher,
            OrderEventDbService orderEventDbService,
            PaymentEventDbService paymentEventDbService,
            @Qualifier(EventDeliveryConfiguration.EVENT_RETRY_SCHEDULER) TaskScheduler retryScheduler,
            EventDeliveryProperties properties,
            EventDeliveryCircuitBreaker circuitBreaker) {
        this.publisher = publisher;
        this.orderEventDbService = orderEventDbService;
        this.paymentEventDbService = paymentEventDbService;
        this.retryScheduler = retryScheduler;
        this.properties = properties;
        this.circuitBreaker = circuitBreaker;
    }

    public void deliverOrderEvent(OrderEventDO event) {
        if (event == null || event.getId() == null) return;
        if (!publishOrder(event, "initial")) {
            scheduleOrderRetry(event.getId(), 0);
        }
    }

    public void deliverPaymentEvent(PaymentEventDO event) {
        if (event == null || event.getId() == null) return;
        if (!publishPayment(event, "initial")) {
            schedulePaymentRetry(event.getId(), 0);
        }
    }

    public List<EventDeliveryVO> listUnacked(
            Integer contentType, int limit, List<Long> eventIds, long afterEventId) {
        validateQuery(contentType, limit, eventIds, afterEventId);
        int queryLimit = Math.min(limit, MAX_QUERY_SIZE);
        LocalDateTime cutoff = LocalDateTime.now().minus(realtimeGracePeriod());
        if (contentType == ContentType.ORDER.getCode()) {
            LambdaQueryWrapper<OrderEventDO> query = new LambdaQueryWrapper<OrderEventDO>()
                    .eq(OrderEventDO::getAcked, EventAckStatus.INIT.getCode())
                    .le(OrderEventDO::getCreateTime, cutoff)
                    .orderByAsc(OrderEventDO::getId).last("LIMIT " + queryLimit);
            applyOrderSelector(query, eventIds, afterEventId);
            return orderEventDbService.list(query).stream().map(this::toVO).toList();
        }
        LambdaQueryWrapper<PaymentEventDO> query = new LambdaQueryWrapper<PaymentEventDO>()
                .eq(PaymentEventDO::getAcked, EventAckStatus.INIT.getCode())
                .le(PaymentEventDO::getCreateTime, cutoff)
                .orderByAsc(PaymentEventDO::getId).last("LIMIT " + queryLimit);
        applyPaymentSelector(query, eventIds, afterEventId);
        return paymentEventDbService.list(query).stream().map(this::toVO).toList();
    }

    private void retryOrder(Long eventId, int retryIndex) {
        OrderEventDO event = orderEventDbService.getById(eventId);
        if (event != null && event.getAcked() == EventAckStatus.INIT.getCode()
                && !publishOrder(event, "short-retry")) {
            scheduleOrderRetry(eventId, retryIndex + 1);
        }
    }

    private void retryPayment(Long eventId, int retryIndex) {
        PaymentEventDO event = paymentEventDbService.getById(eventId);
        if (event != null && event.getAcked() == EventAckStatus.INIT.getCode()
                && !publishPayment(event, "short-retry")) {
            schedulePaymentRetry(eventId, retryIndex + 1);
        }
    }

    private void scheduleOrderRetry(Long eventId, int retryIndex) {
        scheduleRetry(index -> retryOrder(eventId, index), eventId, "order", retryIndex);
    }

    private void schedulePaymentRetry(Long eventId, int retryIndex) {
        scheduleRetry(index -> retryPayment(eventId, index), eventId, "payment", retryIndex);
    }

    private void scheduleRetry(IntConsumer retryAction, Long eventId, String type, int retryIndex) {
        List<Duration> delays = properties.getRetryDelays();
        if (delays == null) return;
        int scheduledIndex = retryIndex;
        while (scheduledIndex < delays.size()) {
            Duration candidate = delays.get(scheduledIndex);
            if (candidate != null && !candidate.isNegative()) break;
            scheduledIndex++;
        }
        if (scheduledIndex >= delays.size()) return;
        Duration delay = delays.get(scheduledIndex);
        int currentRetryIndex = scheduledIndex;
        try {
            retryScheduler.schedule(() -> retryAction.accept(currentRetryIndex), Instant.now().plus(delay));
        } catch (RuntimeException e) {
            log.error("[Redis Stream] ❌ A finite notification retry could not be scheduled; Pipeline MySQL "
                    + "pull remains the fallback. type={}, eventId={}, delay={}", type, eventId, delay, e);
        }
    }

    private boolean publishOrder(OrderEventDO event, String trigger) {
        if (!circuitBreaker.allowPublish(ContentType.ORDER.getCode())) return false;
        try {
            publisher.publishOrderEvent(event);
            return true;
        } catch (RuntimeException e) {
            circuitBreaker.recordPublishFailure(ContentType.ORDER.getCode(), e);
            log.warn("[Redis Stream] 🔄 Order notification failed; event remains available for MySQL pull. "
                    + "eventId={}, trigger={}", event.getId(), trigger, e);
            return false;
        }
    }

    private boolean publishPayment(PaymentEventDO event, String trigger) {
        if (!circuitBreaker.allowPublish(ContentType.PAYMENT.getCode())) return false;
        try {
            publisher.publishPaymentEvent(event);
            return true;
        } catch (RuntimeException e) {
            circuitBreaker.recordPublishFailure(ContentType.PAYMENT.getCode(), e);
            log.warn("[Redis Stream] 🔄 Payment notification failed; event remains available for MySQL pull. "
                    + "eventId={}, trigger={}", event.getId(), trigger, e);
            return false;
        }
    }

    private Duration realtimeGracePeriod() {
        Duration grace = properties.getRealtimeGracePeriod();
        if (grace == null || grace.isNegative()) {
            throw new IllegalArgumentException("trade.ingress.event-delivery.realtime-grace-period不能为负数");
        }
        return grace;
    }

    private static void validateQuery(Integer contentType, int limit, List<Long> eventIds, long afterEventId) {
        if (contentType == null || (contentType != ContentType.ORDER.getCode()
                && contentType != ContentType.PAYMENT.getCode())) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "不支持的事件类型");
        }
        if (limit <= 0 || limit > MAX_QUERY_SIZE || afterEventId < 0
                || (afterEventId > 0 && eventIds != null && !eventIds.isEmpty())) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "limit、afterEventId或eventIds无效");
        }
        if (eventIds != null && (eventIds.size() > MAX_QUERY_SIZE
                || eventIds.stream().anyMatch(id -> id == null || id <= 0))) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "eventIds无效或超过500条");
        }
    }

    private static void applyOrderSelector(LambdaQueryWrapper<OrderEventDO> query, List<Long> ids, long cursor) {
        if (ids != null && !ids.isEmpty()) query.in(OrderEventDO::getId, ids);
        else if (cursor > 0) query.gt(OrderEventDO::getId, cursor);
    }

    private static void applyPaymentSelector(LambdaQueryWrapper<PaymentEventDO> query, List<Long> ids, long cursor) {
        if (ids != null && !ids.isEmpty()) query.in(PaymentEventDO::getId, ids);
        else if (cursor > 0) query.gt(PaymentEventDO::getId, cursor);
    }

    private EventDeliveryVO toVO(OrderEventDO event) {
        return new EventDeliveryVO(ContentType.ORDER.getCode(), event.getId(), event.getSourceSystem(),
                event.getThirdEventKey(), event.getMessageVersion(), event.getRawId(),
                HexFormat.of().formatHex(event.getPayloadSha256()), event.getCreateTime());
    }

    private EventDeliveryVO toVO(PaymentEventDO event) {
        return new EventDeliveryVO(ContentType.PAYMENT.getCode(), event.getId(), event.getSourceSystem(),
                event.getThirdEventKey(), event.getMessageVersion(), event.getRawId(),
                HexFormat.of().formatHex(event.getPayloadSha256()), event.getCreateTime());
    }
}
