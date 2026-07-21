package com.mtx.trade.ingress.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mtx.trade.ingress.common.enums.EventAckStatus;
import com.mtx.trade.ingress.config.EventDeliveryProperties;
import com.mtx.trade.ingress.constants.RedisKeyConstants;
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
                    .le(OrderEventDO::getCreateTime, cutoff)
                    .gt(OrderEventDO::getId, lastId)
                    .orderByAsc(OrderEventDO::getId)
                    .last("LIMIT " + batchSize()));
            for (OrderEventDO event : events) {
                publishOrderSafely(event, "stale-redelivery");
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
                    .le(PaymentEventDO::getCreateTime, cutoff)
                    .gt(PaymentEventDO::getId, lastId)
                    .orderByAsc(PaymentEventDO::getId)
                    .last("LIMIT " + batchSize()));
            for (PaymentEventDO event : events) {
                publishPaymentSafely(event, "stale-redelivery");
            }
            if (events.size() < batchSize()) {
                return;
            }
            lastId = events.get(events.size() - 1).getId();
        }
    }

    private void publishOrderSafely(OrderEventDO event, String reason) {
        try {
            eventStreamPublisher.publishOrderEvent(event);
        } catch (RuntimeException e) {
            log.warn("order event publish failed, eventId={}, reason={}", event.getId(), reason, e);
        }
    }

    private void publishPaymentSafely(PaymentEventDO event, String reason) {
        try {
            eventStreamPublisher.publishPaymentEvent(event);
        } catch (RuntimeException e) {
            log.warn("payment event publish failed, eventId={}, reason={}", event.getId(), reason, e);
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

    private static int durationMillisAsInt(Duration duration) {
        long millis = duration == null ? 1L : Math.max(1L, duration.toMillis());
        return (int) Math.min(Integer.MAX_VALUE, millis);
    }
}
