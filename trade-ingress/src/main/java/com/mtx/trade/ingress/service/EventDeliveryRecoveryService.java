package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.config.EventDeliveryCircuitProperties;
import com.mtx.trade.ingress.entity.EventDeliveryControlDO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/** 检查 Redis 发布能力；不调用 Pipeline，不重推历史事件。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventDeliveryRecoveryService {
    private final EventDeliveryCircuitBreaker circuitBreaker;
    private final EventDeliveryCircuitProperties properties;
    private final EventStreamPublisher eventStreamPublisher;

    public void recoverPublishing() {
        if (!properties.isEnabled()) return;
        for (EventDeliveryControlDO state : circuitBreaker.listDueForHealthCheck()) {
            int contentType = state.getContentType();
            if (!circuitBreaker.claimHealthCheck(contentType)) continue;
            try {
                if (!eventStreamPublisher.readyToResume(contentType)) {
                    throw new IllegalStateException(
                            "Redis, consumer group or stream low-watermark readiness check failed");
                }
                circuitBreaker.recordHealthSuccess(contentType);
            } catch (Exception e) {
                circuitBreaker.recordHealthFailure(contentType, e);
                log.warn("[Circuit Breaker] 🔄 Redis is still unavailable; publishing remains paused. "
                        + "contentType={}", contentType, e);
            }
        }
    }
}
