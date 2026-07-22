package com.mtx.trade.ingress.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.common.enums.EventAckStatus;
import com.mtx.trade.ingress.config.EventDeliveryConfiguration;
import com.mtx.trade.ingress.config.EventDeliveryProperties;
import com.mtx.trade.ingress.constants.RedisKeyConstants;
import com.mtx.trade.ingress.dto.EventDeliveryVO;
import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.entity.PaymentEventDO;
import com.mtx.trade.ingress.service.db.OrderEventDbService;
import com.mtx.trade.ingress.service.db.PaymentEventDbService;
import com.mtx.trade.ingress.utils.RedisLock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;

/** Ingress 事件立即投递、短期重试和超时补发服务。 */
@Slf4j
@Service
public class EventDeliveryService {

    private final EventStreamPublisher eventStreamPublisher;
    private final OrderEventDbService orderEventDbService;
    private final PaymentEventDbService paymentEventDbService;
    private final TaskScheduler taskScheduler;
    private final RedisLock redisLock;
    private final EventDeliveryProperties properties;
    private final EventDeliveryCircuitBreaker circuitBreaker;

    public EventDeliveryService(
            EventStreamPublisher eventStreamPublisher,
            OrderEventDbService orderEventDbService,
            PaymentEventDbService paymentEventDbService,
            @Qualifier(EventDeliveryConfiguration.EVENT_RETRY_SCHEDULER) TaskScheduler taskScheduler,
            RedisLock redisLock,
            EventDeliveryProperties properties,
            EventDeliveryCircuitBreaker circuitBreaker) {
        this.eventStreamPublisher = eventStreamPublisher;
        this.orderEventDbService = orderEventDbService;
        this.paymentEventDbService = paymentEventDbService;
        this.taskScheduler = taskScheduler;
        this.redisLock = redisLock;
        this.properties = properties;
        this.circuitBreaker = circuitBreaker;
    }

    public void deliverOrderEvent(OrderEventDO event) {
        if (event == null || event.getId() == null) {
            return;
        }
        publishOrderSafely(event, "initial");
        if (circuitBreaker.allowNormalPublish(ContentType.ORDER.getCode())) {
            scheduleOrderRetries(event.getId());
        }
    }

    public void deliverPaymentEvent(PaymentEventDO event) {
        if (event == null || event.getId() == null) {
            return;
        }
        publishPaymentSafely(event, "initial");
        if (circuitBreaker.allowNormalPublish(ContentType.PAYMENT.getCode())) {
            schedulePaymentRetries(event.getId());
        }
    }

    /** 查询自动补发已耗尽、等待人工处理的事件。 */
    public List<EventDeliveryVO> listExhausted(
            Integer contentType, int limit, List<Long> eventIds, long afterEventId) {
        validateContentType(contentType);
        if (afterEventId < 0 || (afterEventId > 0 && eventIds != null && !eventIds.isEmpty())) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "afterEventId无效或不能与eventIds同时使用");
        }
        int queryLimit = Math.max(1, Math.min(limit, 500));
        if (contentType == ContentType.ORDER.getCode()) {
            LambdaQueryWrapper<OrderEventDO> wrapper = new LambdaQueryWrapper<OrderEventDO>()
                    .eq(OrderEventDO::getAcked, EventAckStatus.INIT.getCode())
                    .ge(OrderEventDO::getAutoRedeliveryCount, maxAutoRedeliveries())
                    .orderByAsc(OrderEventDO::getId)
                    .last("LIMIT " + queryLimit);
            if (eventIds != null && !eventIds.isEmpty()) {
                wrapper.in(OrderEventDO::getId, eventIds);
            } else if (afterEventId > 0) {
                wrapper.gt(OrderEventDO::getId, afterEventId);
            }
            return orderEventDbService.list(wrapper)
                    .stream().map(this::toDeliveryVO).toList();
        }
        LambdaQueryWrapper<PaymentEventDO> wrapper = new LambdaQueryWrapper<PaymentEventDO>()
                .eq(PaymentEventDO::getAcked, EventAckStatus.INIT.getCode())
                .ge(PaymentEventDO::getAutoRedeliveryCount, maxAutoRedeliveries())
                .orderByAsc(PaymentEventDO::getId)
                .last("LIMIT " + queryLimit);
        if (eventIds != null && !eventIds.isEmpty()) {
            wrapper.in(PaymentEventDO::getId, eventIds);
        } else if (afterEventId > 0) {
            wrapper.gt(PaymentEventDO::getId, afterEventId);
        }
        return paymentEventDbService.list(wrapper)
                .stream().map(this::toDeliveryVO).toList();
    }

    /** 人工发布一次，不重置自动补发次数。 */
    public void manualRedeliver(Integer contentType, Long eventId) {
        validateCommand(contentType, eventId);
        if (contentType == ContentType.ORDER.getCode()) {
            OrderEventDO event = requireUnackedOrder(eventId);
            try {
                eventStreamPublisher.publishOrderEvent(event);
            } catch (RuntimeException e) {
                circuitBreaker.recordPublishFailure(contentType, e);
                throw e;
            }
            return;
        }
        PaymentEventDO event = requireUnackedPayment(eventId);
        try {
            eventStreamPublisher.publishPaymentEvent(event);
        } catch (RuntimeException e) {
            circuitBreaker.recordPublishFailure(contentType, e);
            throw e;
        }
    }

    /** 恢复自动补发资格，等待下一轮扫描。 */
    public void resumeAutoRedelivery(Integer contentType, Long eventId) {
        validateCommand(contentType, eventId);
        boolean updated;
        if (contentType == ContentType.ORDER.getCode()) {
            updated = orderEventDbService.update(new LambdaUpdateWrapper<OrderEventDO>()
                    .eq(OrderEventDO::getId, eventId)
                    .eq(OrderEventDO::getAcked, EventAckStatus.INIT.getCode())
                    .ge(OrderEventDO::getAutoRedeliveryCount, maxAutoRedeliveries())
                    .set(OrderEventDO::getAutoRedeliveryCount, 0)
                    .set(OrderEventDO::getLastRedeliveryTime, null));
        } else {
            updated = paymentEventDbService.update(new LambdaUpdateWrapper<PaymentEventDO>()
                    .eq(PaymentEventDO::getId, eventId)
                    .eq(PaymentEventDO::getAcked, EventAckStatus.INIT.getCode())
                    .ge(PaymentEventDO::getAutoRedeliveryCount, maxAutoRedeliveries())
                    .set(PaymentEventDO::getAutoRedeliveryCount, 0)
                    .set(PaymentEventDO::getLastRedeliveryTime, null));
        }
        if (!updated) {
            if (contentType == ContentType.ORDER.getCode()) {
                requireUnackedOrder(eventId);
            } else {
                requireUnackedPayment(eventId);
            }
        }
    }

    public void redeliverStaleUnackedEvents() {
        if (!circuitBreaker.allowNormalPublish(ContentType.ORDER.getCode())
                && !circuitBreaker.allowNormalPublish(ContentType.PAYMENT.getCode())) {
            return;
        }
        int leaseMillis = durationMillisAsInt(properties.getScanLockLease());
        boolean locked;
        try {
            locked = redisLock.acquireInstantLock(
                    RedisKeyConstants.EVENT_REDELIVERY_JOB_LOCK, leaseMillis, TimeUnit.MILLISECONDS);
        } catch (RuntimeException e) {
            circuitBreaker.recordPublishFailure(ContentType.ORDER.getCode(), e);
            circuitBreaker.recordPublishFailure(ContentType.PAYMENT.getCode(), e);
            log.error("[Scheduled Redelivery] ❌ Redis job lock is unavailable; this scan was skipped.", e);
            return;
        }
        if (!locked) {
            log.debug("[Scheduled Redelivery] 🔄 Another Ingress instance owns the scan lease; this run was skipped.");
            return;
        }
        try {
            LocalDateTime cutoff = LocalDateTime.now().minus(properties.getStaleAfter());
            log.info("[Scheduled Redelivery] 🔄 Scanning stale unacknowledged events. cutoff={}", cutoff);
            scanOrderEvents(cutoff);
            scanPaymentEvents(cutoff);
            log.info("[Scheduled Redelivery] ✅ Scheduled scan completed. cutoff={}", cutoff);
        } finally {
            redisLock.releaseLock(RedisKeyConstants.EVENT_REDELIVERY_JOB_LOCK);
        }
    }

    private void scheduleOrderRetries(Long eventId) {
        for (Duration delay : properties.getRetryDelays()) {
            schedule(() -> retryOrderEvent(eventId), delay, eventId, "order");
        }
    }

    private void schedulePaymentRetries(Long eventId) {
        for (Duration delay : properties.getRetryDelays()) {
            schedule(() -> retryPaymentEvent(eventId), delay, eventId, "payment");
        }
    }

    private void schedule(Runnable task, Duration delay, Long eventId, String eventType) {
        if (delay == null || delay.isNegative()) {
            return;
        }
        try {
            taskScheduler.schedule(task, Instant.now().plus(delay));
        } catch (RuntimeException e) {
            log.error("[Scheduled Redelivery] ❌ Retry could not be scheduled; the periodic scan remains the "
                    + "fallback. type={}, eventId={}, delay={}", eventType, eventId, delay, e);
        }
    }

    private void retryOrderEvent(Long eventId) {
        OrderEventDO event = orderEventDbService.getById(eventId);
        if (isUnacked(event == null ? null : event.getAcked())) {
            publishOrderSafely(event, "short-retry");
        }
    }

    private void retryPaymentEvent(Long eventId) {
        PaymentEventDO event = paymentEventDbService.getById(eventId);
        if (isUnacked(event == null ? null : event.getAcked())) {
            publishPaymentSafely(event, "short-retry");
        }
    }

    private void scanOrderEvents(LocalDateTime cutoff) {
        long lastId = 0L;
        for (int batch = 0; batch < maxBatches(); batch++) {
            if (!circuitBreaker.allowNormalPublish(ContentType.ORDER.getCode())) {
                return;
            }
            List<OrderEventDO> events = orderEventDbService.list(new LambdaQueryWrapper<OrderEventDO>()
                    .eq(OrderEventDO::getAcked, EventAckStatus.INIT.getCode())
                    .lt(OrderEventDO::getAutoRedeliveryCount, maxAutoRedeliveries())
                    .le(OrderEventDO::getCreateTime, cutoff)
                    .gt(OrderEventDO::getId, lastId)
                    .orderByAsc(OrderEventDO::getId)
                    .last("LIMIT " + batchSize()));
            for (OrderEventDO event : events) {
                if (publishOrderSafely(event, "stale-redelivery")) {
                    recordOrderAutoRedelivery(event);
                } else if (!circuitBreaker.allowNormalPublish(ContentType.ORDER.getCode())) {
                    return;
                }
            }
            if (events.size() < batchSize()) {
                return;
            }
            lastId = events.get(events.size() - 1).getId();
        }
    }

    private void scanPaymentEvents(LocalDateTime cutoff) {
        long lastId = 0L;
        for (int batch = 0; batch < maxBatches(); batch++) {
            if (!circuitBreaker.allowNormalPublish(ContentType.PAYMENT.getCode())) {
                return;
            }
            List<PaymentEventDO> events = paymentEventDbService.list(new LambdaQueryWrapper<PaymentEventDO>()
                    .eq(PaymentEventDO::getAcked, EventAckStatus.INIT.getCode())
                    .lt(PaymentEventDO::getAutoRedeliveryCount, maxAutoRedeliveries())
                    .le(PaymentEventDO::getCreateTime, cutoff)
                    .gt(PaymentEventDO::getId, lastId)
                    .orderByAsc(PaymentEventDO::getId)
                    .last("LIMIT " + batchSize()));
            for (PaymentEventDO event : events) {
                if (publishPaymentSafely(event, "stale-redelivery")) {
                    recordPaymentAutoRedelivery(event);
                } else if (!circuitBreaker.allowNormalPublish(ContentType.PAYMENT.getCode())) {
                    return;
                }
            }
            if (events.size() < batchSize()) {
                return;
            }
            lastId = events.get(events.size() - 1).getId();
        }
    }

    private boolean publishOrderSafely(OrderEventDO event, String reason) {
        if (!circuitBreaker.allowNormalPublish(ContentType.ORDER.getCode())) {
            log.debug("[Circuit Breaker] 🔄 Order publication is paused while the circuit is OPEN. "
                    + "eventId={}, trigger={}", event.getId(), reason);
            return false;
        }
        try {
            eventStreamPublisher.publishOrderEvent(event);
            return true;
        } catch (RuntimeException e) {
            circuitBreaker.recordPublishFailure(ContentType.ORDER.getCode(), e);
            log.error("[Redis Stream] ❌ Order event publication failed; the event remains unacknowledged. "
                    + "eventId={}, trigger={}", event.getId(), reason, e);
            return false;
        }
    }

    private boolean publishPaymentSafely(PaymentEventDO event, String reason) {
        if (!circuitBreaker.allowNormalPublish(ContentType.PAYMENT.getCode())) {
            log.debug("[Circuit Breaker] 🔄 Payment publication is paused while the circuit is OPEN. "
                    + "eventId={}, trigger={}", event.getId(), reason);
            return false;
        }
        try {
            eventStreamPublisher.publishPaymentEvent(event);
            return true;
        } catch (RuntimeException e) {
            circuitBreaker.recordPublishFailure(ContentType.PAYMENT.getCode(), e);
            log.error("[Redis Stream] ❌ Payment event publication failed; the event remains unacknowledged. "
                    + "eventId={}, trigger={}", event.getId(), reason, e);
            return false;
        }
    }

    /** HALF_OPEN 专用探测发布；返回等待 Pipeline ACK 的 event ID。 */
    public Long publishHalfOpenProbe(int contentType) {
        if (contentType == ContentType.ORDER.getCode()) {
            OrderEventDO event = orderEventDbService.getOne(new LambdaQueryWrapper<OrderEventDO>()
                    .eq(OrderEventDO::getAcked, EventAckStatus.INIT.getCode())
                    .orderByAsc(OrderEventDO::getId)
                    .last("LIMIT 1"), false);
            if (event == null) {
                return null;
            }
            eventStreamPublisher.publishOrderEvent(event);
            return event.getId();
        }
        PaymentEventDO event = paymentEventDbService.getOne(new LambdaQueryWrapper<PaymentEventDO>()
                .eq(PaymentEventDO::getAcked, EventAckStatus.INIT.getCode())
                .orderByAsc(PaymentEventDO::getId)
                .last("LIMIT 1"), false);
        if (event == null) {
            return null;
        }
        eventStreamPublisher.publishPaymentEvent(event);
        return event.getId();
    }

    public boolean isEventAcked(int contentType, long eventId) {
        if (contentType == ContentType.ORDER.getCode()) {
            OrderEventDO event = orderEventDbService.getById(eventId);
            return event != null && Objects.equals(event.getAcked(), EventAckStatus.ACKED.getCode());
        }
        PaymentEventDO event = paymentEventDbService.getById(eventId);
        return event != null && Objects.equals(event.getAcked(), EventAckStatus.ACKED.getCode());
    }

    public long maxUnackedEventId(int contentType) {
        if (contentType == ContentType.ORDER.getCode()) {
            OrderEventDO event = orderEventDbService.getOne(new LambdaQueryWrapper<OrderEventDO>()
                    .eq(OrderEventDO::getAcked, EventAckStatus.INIT.getCode())
                    .orderByDesc(OrderEventDO::getId)
                    .last("LIMIT 1"), false);
            return event == null ? 0L : event.getId();
        }
        PaymentEventDO event = paymentEventDbService.getOne(new LambdaQueryWrapper<PaymentEventDO>()
                .eq(PaymentEventDO::getAcked, EventAckStatus.INIT.getCode())
                .orderByDesc(PaymentEventDO::getId)
                .last("LIMIT 1"), false);
        return event == null ? 0L : event.getId();
    }

    /** 熔断关闭后按游标恢复积压；每条按一次新的首次投递处理并重新安排短期重试。 */
    public RecoveryBatchResult recoverBacklog(int contentType, long cursorId, long cutoffId, int limit) {
        long cursor = cursorId;
        int normalizedLimit = Math.max(1, limit);
        if (contentType == ContentType.ORDER.getCode()) {
            List<OrderEventDO> events = orderEventDbService.list(new LambdaQueryWrapper<OrderEventDO>()
                    .eq(OrderEventDO::getAcked, EventAckStatus.INIT.getCode())
                    .gt(OrderEventDO::getId, cursorId)
                    .le(OrderEventDO::getId, cutoffId)
                    .orderByAsc(OrderEventDO::getId)
                    .last("LIMIT " + normalizedLimit));
            for (OrderEventDO event : events) {
                if (!circuitBreaker.allowNormalPublish(contentType)) {
                    return new RecoveryBatchResult(cursor, false);
                }
                deliverOrderEvent(event);
                if (!circuitBreaker.allowNormalPublish(contentType)) {
                    return new RecoveryBatchResult(cursor, false);
                }
                cursor = event.getId();
            }
            return new RecoveryBatchResult(cursor, events.size() < normalizedLimit || cursor >= cutoffId);
        }
        List<PaymentEventDO> events = paymentEventDbService.list(new LambdaQueryWrapper<PaymentEventDO>()
                .eq(PaymentEventDO::getAcked, EventAckStatus.INIT.getCode())
                .gt(PaymentEventDO::getId, cursorId)
                .le(PaymentEventDO::getId, cutoffId)
                .orderByAsc(PaymentEventDO::getId)
                .last("LIMIT " + normalizedLimit));
        for (PaymentEventDO event : events) {
            if (!circuitBreaker.allowNormalPublish(contentType)) {
                return new RecoveryBatchResult(cursor, false);
            }
            deliverPaymentEvent(event);
            if (!circuitBreaker.allowNormalPublish(contentType)) {
                return new RecoveryBatchResult(cursor, false);
            }
            cursor = event.getId();
        }
        return new RecoveryBatchResult(cursor, events.size() < normalizedLimit || cursor >= cutoffId);
    }

    public record RecoveryBatchResult(long cursorId, boolean finished) {
    }

    private void recordOrderAutoRedelivery(OrderEventDO event) {
        boolean updated = orderEventDbService.update(new LambdaUpdateWrapper<OrderEventDO>()
                .eq(OrderEventDO::getId, event.getId())
                .eq(OrderEventDO::getAcked, EventAckStatus.INIT.getCode())
                .lt(OrderEventDO::getAutoRedeliveryCount, maxAutoRedeliveries())
                .setSql("auto_redelivery_count = auto_redelivery_count + 1")
                .set(OrderEventDO::getLastRedeliveryTime, LocalDateTime.now()));
        logIfExhausted(updated, event.getAutoRedeliveryCount(), "order", event.getId());
    }

    private void recordPaymentAutoRedelivery(PaymentEventDO event) {
        boolean updated = paymentEventDbService.update(new LambdaUpdateWrapper<PaymentEventDO>()
                .eq(PaymentEventDO::getId, event.getId())
                .eq(PaymentEventDO::getAcked, EventAckStatus.INIT.getCode())
                .lt(PaymentEventDO::getAutoRedeliveryCount, maxAutoRedeliveries())
                .setSql("auto_redelivery_count = auto_redelivery_count + 1")
                .set(PaymentEventDO::getLastRedeliveryTime, LocalDateTime.now()));
        logIfExhausted(updated, event.getAutoRedeliveryCount(), "payment", event.getId());
    }

    private void logIfExhausted(boolean updated, Integer currentCount, String eventType, Long eventId) {
        int count = currentCount == null ? 0 : currentCount;
        if (updated && count + 1 >= maxAutoRedeliveries()) {
            log.warn("[Scheduled Redelivery] 🔄 Scheduled redelivery limit reached after a successful publish; "
                            + "the event now waits for Pipeline exhausted-event pull. "
                            + "type={}, eventId={}, scheduledAttempts={}",
                    eventType, eventId, count + 1);
        }
    }

    private OrderEventDO requireUnackedOrder(Long eventId) {
        OrderEventDO event = orderEventDbService.getById(eventId);
        if (event == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND, "订单事件不存在");
        }
        if (!isUnacked(event.getAcked())) {
            throw new BusinessException(ErrorCode.BUSINESS_ERROR, "订单事件已ACK，无需重推");
        }
        return event;
    }

    private PaymentEventDO requireUnackedPayment(Long eventId) {
        PaymentEventDO event = paymentEventDbService.getById(eventId);
        if (event == null) {
            throw new BusinessException(ErrorCode.NOT_FOUND, "支付事件不存在");
        }
        if (!isUnacked(event.getAcked())) {
            throw new BusinessException(ErrorCode.BUSINESS_ERROR, "支付事件已ACK，无需重推");
        }
        return event;
    }

    private EventDeliveryVO toDeliveryVO(OrderEventDO event) {
        return new EventDeliveryVO(ContentType.ORDER.getCode(), event.getId(), event.getSourceSystem(),
                event.getThirdEventKey(), event.getMessageVersion(), event.getRawId(),
                HexFormat.of().formatHex(event.getPayloadSha256()),
                event.getAutoRedeliveryCount(), event.getLastRedeliveryTime(), event.getCreateTime());
    }

    private EventDeliveryVO toDeliveryVO(PaymentEventDO event) {
        return new EventDeliveryVO(ContentType.PAYMENT.getCode(), event.getId(), event.getSourceSystem(),
                event.getThirdEventKey(), event.getMessageVersion(), event.getRawId(),
                HexFormat.of().formatHex(event.getPayloadSha256()),
                event.getAutoRedeliveryCount(), event.getLastRedeliveryTime(), event.getCreateTime());
    }

    private static void validateCommand(Integer contentType, Long eventId) {
        validateContentType(contentType);
        if (eventId == null || eventId <= 0) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "eventId 无效");
        }
    }

    private static void validateContentType(Integer contentType) {
        if (contentType == null
                || contentType != ContentType.ORDER.getCode() && contentType != ContentType.PAYMENT.getCode()) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "不支持的事件类型");
        }
    }

    private static boolean isUnacked(Integer acked) {
        return Objects.equals(acked, EventAckStatus.INIT.getCode());
    }

    private int batchSize() {
        return Math.max(1, properties.getBatchSize());
    }

    private int maxBatches() {
        return Math.max(1, properties.getMaxBatches());
    }

    private int maxAutoRedeliveries() {
        return Math.max(1, properties.getMaxAutoRedeliveries());
    }

    private static int durationMillisAsInt(Duration duration) {
        long millis = duration == null ? 1L : Math.max(1L, duration.toMillis());
        return (int) Math.min(Integer.MAX_VALUE, millis);
    }
}
