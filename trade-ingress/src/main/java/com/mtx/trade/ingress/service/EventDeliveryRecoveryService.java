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
        for (EventDeliveryControlDO control : circuitBreaker.listClosedForHealthCheck()) {
            checkClosedCircuit(control.getContentType());
        }
        for (EventDeliveryControlDO control : circuitBreaker.listDueForHealthCheck()) {
            if (control.getCircuitStatus() == DeliveryCircuitStatus.OPEN.getCode()) {
                recoverOpenCircuit(control.getContentType());
            } else {
                recoverHalfOpenCircuit(control.getContentType());
            }
        }
        for (EventDeliveryControlDO control : circuitBreaker.listDrainable()) {
            drainBacklog(control);
        }
    }

    private void checkClosedCircuit(int contentType) {
        if (!circuitBreaker.claim(contentType, DeliveryCircuitStatus.CLOSED.getCode())) {
            return;
        }
        try {
            if (!pipelineReadinessClient.isReady(contentType)) {
                throw new IllegalStateException("Pipeline event consumer is not ready");
            }
            circuitBreaker.recordClosedPipelineReady(contentType);
        } catch (Exception e) {
            circuitBreaker.recordClosedPipelineFailure(contentType, e);
            log.warn("[Circuit Breaker] 🔄 Pipeline readiness check failed; waiting before the next check. "
                            + "contentType={}, reason={}",
                    contentType, e.getMessage());
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
                startHalfOpenProbe(contentType);
            }
        } catch (Exception e) {
            circuitBreaker.recordHealthFailure(contentType, e);
            log.warn("[Circuit Breaker] 🔄 Recovery check is still failing; circuit remains OPEN. "
                            + "contentType={}, reason={}",
                    contentType, e.getMessage());
        }
    }

    private void startHalfOpenProbe(int contentType) {
        try {
            Long probeEventId = eventDeliveryService.publishHalfOpenProbe(contentType);
            if (probeEventId == null) {
                log.info("[Circuit Breaker] ✅ No unacknowledged event requires a probe; closing the circuit. "
                        + "contentType={}", contentType);
                circuitBreaker.closeAfterProbe(contentType, 0L);
                return;
            }
            log.warn("[Circuit Breaker] 🔄 Probe event published; waiting for Pipeline ACK. "
                    + "contentType={}, eventId={}", contentType, probeEventId);
            circuitBreaker.waitForProbeAck(contentType, probeEventId);
        } catch (Exception e) {
            circuitBreaker.reopenAfterProbeFailure(contentType, e);
        }
    }

    private void recoverHalfOpenCircuit(int contentType) {
        if (!circuitBreaker.claim(contentType, DeliveryCircuitStatus.HALF_OPEN.getCode())) {
            return;
        }
        try {
            if (!pipelineReadinessClient.isReady(contentType)) {
                throw new IllegalStateException("Pipeline event consumer is not ready");
            }
            EventDeliveryControlDO control = circuitBreaker.getControl(contentType);
            Long probeEventId = control == null ? null : control.getProbeEventId();
            if (probeEventId == null) {
                startHalfOpenProbe(contentType);
                return;
            }
            if (eventDeliveryService.isEventAcked(contentType, probeEventId)) {
                log.info("[Circuit Breaker] ✅ Probe ACK received; closing the circuit. "
                        + "contentType={}, eventId={}", contentType, probeEventId);
                circuitBreaker.closeAfterProbe(contentType, eventDeliveryService.maxUnackedEventId(contentType));
            } else {
                log.warn("[Circuit Breaker] 🔄 Probe ACK is still pending; circuit remains HALF_OPEN. "
                        + "contentType={}, eventId={}", contentType, probeEventId);
                circuitBreaker.waitForProbeAck(contentType, probeEventId);
            }
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
            if (result.finished()) {
                log.info("[Backlog Recovery] ✅ Backlog recovery completed. contentType={}, cursorId={}",
                        contentType, result.cursorId());
            } else {
                log.info("[Backlog Recovery] 🔄 Recovery batch completed; more backlog remains. "
                        + "contentType={}, cursorId={}", contentType, result.cursorId());
            }
        } catch (Exception e) {
            circuitBreaker.recordPublishFailure(contentType, e);
            circuitBreaker.advanceRecoveryCursor(
                    contentType,
                    control.getRecoveryCursorId() == null ? 0L : control.getRecoveryCursorId(),
                    false);
            log.error("[Backlog Recovery] ❌ Backlog recovery batch failed; the cursor is retained for retry. "
                    + "contentType={}", contentType, e);
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
