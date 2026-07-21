package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.common.enums.DeliveryCircuitStatus;
import com.mtx.trade.ingress.config.EventDeliveryCircuitProperties;
import com.mtx.trade.ingress.entity.EventDeliveryControlDO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/** 持有MySQL租约执行熔断探活、HALF_OPEN试投以及关闭后的限批积压恢复。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventDeliveryRecoveryService {

    private final EventDeliveryCircuitBreaker circuitBreaker;
    private final EventDeliveryService eventDeliveryService;
    private final PipelineReadinessClient pipelineReadinessClient;
    private final EventDeliveryCircuitProperties properties;
    private final StringRedisTemplate redisTemplate;

    @Scheduled(fixedDelayString = "${trade.ingress.event-delivery.circuit.scheduler-delay-ms:30000}")
    public void recoverCircuitsAndDrainBacklog() {
        if (!properties.isEnabled()) {
            return;
        }
        for (EventDeliveryControlDO control : circuitBreaker.listDueForHealthCheck()) {
            recoverOpenCircuit(control.getContentType());
        }
        for (EventDeliveryControlDO control : circuitBreaker.listDrainable()) {
            drainBacklog(control);
        }
    }

    private void recoverOpenCircuit(int contentType) {
        if (!circuitBreaker.claim(contentType, DeliveryCircuitStatus.OPEN.getCode())) {
            return;
        }
        try {
            if (!redisReady()) {
                throw new IllegalStateException("Ingress Redis PING failed");
            }
            if (!pipelineReadinessClient.isReady(contentType)) {
                throw new IllegalStateException("Pipeline event consumer is not ready");
            }
            boolean halfOpen = circuitBreaker.recordHealthSuccess(contentType);
            if (halfOpen) {
                probeAndClose(contentType);
            }
        } catch (Exception e) {
            circuitBreaker.recordHealthFailure(contentType, e);
            log.warn("event delivery circuit health check failed, contentType={}", contentType, e);
        }
    }

    private void probeAndClose(int contentType) {
        try {
            eventDeliveryService.publishHalfOpenProbe(
                    contentType, Math.max(1, properties.getHalfOpenPermits()));
            long cutoffId = eventDeliveryService.maxUnackedEventId(contentType);
            circuitBreaker.closeAfterProbe(contentType, cutoffId);
        } catch (Exception e) {
            circuitBreaker.reopenAfterProbeFailure(contentType, e);
        }
    }

    private void drainBacklog(EventDeliveryControlDO snapshot) {
        int contentType = snapshot.getContentType();
        if (!circuitBreaker.claim(contentType, DeliveryCircuitStatus.CLOSED.getCode())) {
            return;
        }
        EventDeliveryControlDO control = circuitBreaker.getControl(contentType);
        if (control == null || control.getRecoveryCutoffId() == null || control.getRecoveryCutoffId() <= 0) {
            circuitBreaker.advanceRecoveryCursor(contentType, 0L, true);
            return;
        }
        try {
            EventDeliveryService.RecoveryBatchResult result = eventDeliveryService.recoverBacklog(
                    contentType,
                    control.getRecoveryCursorId() == null ? 0L : control.getRecoveryCursorId(),
                    control.getRecoveryCutoffId(),
                    Math.max(1, properties.getRecoveryBatchSize()));
            circuitBreaker.advanceRecoveryCursor(contentType, result.cursorId(), result.finished());
        } catch (Exception e) {
            circuitBreaker.recordPublishFailure(contentType, e);
            circuitBreaker.advanceRecoveryCursor(
                    contentType,
                    control.getRecoveryCursorId() == null ? 0L : control.getRecoveryCursorId(),
                    false);
            log.warn("recover event delivery backlog failed, contentType={}", contentType, e);
        }
    }

    private boolean redisReady() {
        RedisConnectionFactory factory = redisTemplate.getConnectionFactory();
        if (factory == null) {
            return false;
        }
        try (RedisConnection connection = factory.getConnection()) {
            String pong = connection.ping();
            return pong != null && "PONG".equalsIgnoreCase(pong);
        } catch (Exception e) {
            return false;
        }
    }
}
