package com.mtx.trade.ingress.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.common.enums.EventAckStatus;
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
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Ingress 事件立即投递、短期重试和超时补发服务。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventDeliveryService {

    private final EventStreamPublisher eventStreamPublisher;
    private final OrderEventDbService orderEventDbService;
    private final PaymentEventDbService paymentEventDbService;
    private final TaskScheduler taskScheduler;
    private final RedisLock redisLock;
    private final EventDeliveryProperties properties;

    public void deliverOrderEvent(OrderEventDO event) {
        if (event == null || event.getId() == null) {
            return;
        }
        publishOrderSafely(event, "initial");
        scheduleOrderRetries(event.getId());
    }

    public void deliverPaymentEvent(PaymentEventDO event) {
        if (event == null || event.getId() == null) {
            return;
        }
        publishPaymentSafely(event, "initial");
        schedulePaymentRetries(event.getId());
    }

    /** 查询自动补发已耗尽、等待人工处理的事件。 */
    public List<EventDeliveryVO> listExhausted(Integer contentType, int limit) {
        validateContentType(contentType);
        int queryLimit = Math.max(1, Math.min(limit, 500));
        if (contentType == ContentType.ORDER.getCode()) {
            return orderEventDbService.list(new LambdaQueryWrapper<OrderEventDO>()
                            .eq(OrderEventDO::getAcked, EventAckStatus.INIT.getCode())
                            .ge(OrderEventDO::getAutoRedeliveryCount, maxAutoRedeliveries())
                            .orderByAsc(OrderEventDO::getCreateTime, OrderEventDO::getId)
                            .last("LIMIT " + queryLimit))
                    .stream().map(this::toDeliveryVO).toList();
        }
        return paymentEventDbService.list(new LambdaQueryWrapper<PaymentEventDO>()
                        .eq(PaymentEventDO::getAcked, EventAckStatus.INIT.getCode())
                        .ge(PaymentEventDO::getAutoRedeliveryCount, maxAutoRedeliveries())
                        .orderByAsc(PaymentEventDO::getCreateTime, PaymentEventDO::getId)
                        .last("LIMIT " + queryLimit))
                .stream().map(this::toDeliveryVO).toList();
    }

    /** 人工发布一次，不重置自动补发次数。 */
    public void manualRedeliver(Integer contentType, Long eventId) {
        validateCommand(contentType, eventId);
        if (contentType == ContentType.ORDER.getCode()) {
            OrderEventDO event = requireUnackedOrder(eventId);
            eventStreamPublisher.publishOrderEvent(event);
            return;
        }
        PaymentEventDO event = requireUnackedPayment(eventId);
        eventStreamPublisher.publishPaymentEvent(event);
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

    @Scheduled(cron = "${trade.ingress.event-delivery.scan-cron:0 */15 * * * *}")
    public void redeliverStaleUnackedEvents() {
        int leaseMillis = durationMillisAsInt(properties.getScanLockLease());
        if (!redisLock.acquireInstantLock(
                RedisKeyConstants.EVENT_REDELIVERY_JOB_LOCK, leaseMillis, TimeUnit.MILLISECONDS)) {
            return;
        }
        try {
            LocalDateTime cutoff = LocalDateTime.now().minus(properties.getStaleAfter());
            scanOrderEvents(cutoff);
            scanPaymentEvents(cutoff);
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
            log.warn("event retry schedule failed, type={}, eventId={}, delay={}", eventType, eventId, delay, e);
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
                }
            }
            if (events.size() < batchSize()) {
                return;
            }
            lastId = events.get(events.size() - 1).getId();
        }
    }

    private boolean publishOrderSafely(OrderEventDO event, String reason) {
        try {
            eventStreamPublisher.publishOrderEvent(event);
            return true;
        } catch (RuntimeException e) {
            log.warn("order event publish failed, eventId={}, reason={}", event.getId(), reason, e);
            return false;
        }
    }

    private boolean publishPaymentSafely(PaymentEventDO event, String reason) {
        try {
            eventStreamPublisher.publishPaymentEvent(event);
            return true;
        } catch (RuntimeException e) {
            log.warn("payment event publish failed, eventId={}, reason={}", event.getId(), reason, e);
            return false;
        }
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
            log.warn("event auto redelivery exhausted, type={}, eventId={}, count={}",
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
                event.getAutoRedeliveryCount(), event.getLastRedeliveryTime(), event.getCreateTime());
    }

    private EventDeliveryVO toDeliveryVO(PaymentEventDO event) {
        return new EventDeliveryVO(ContentType.PAYMENT.getCode(), event.getId(), event.getSourceSystem(),
                event.getThirdEventKey(), event.getMessageVersion(), event.getRawId(),
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
