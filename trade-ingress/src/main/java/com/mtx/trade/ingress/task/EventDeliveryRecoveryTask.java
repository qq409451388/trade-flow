package com.mtx.trade.ingress.task;

import com.mtx.trade.ingress.config.EventDeliveryConfiguration;
import com.mtx.trade.ingress.service.EventDeliveryRecoveryService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 定时探测 Redis 发布能力；恢复后仅放行新的实时通知。 */
@Component
@RequiredArgsConstructor
public class EventDeliveryRecoveryTask {

    private final EventDeliveryRecoveryService recoveryService;

    @Scheduled(
            fixedDelayString = "${trade.ingress.event-delivery.circuit.scheduler-delay-ms:30000}",
            scheduler = EventDeliveryConfiguration.CIRCUIT_RECOVERY_SCHEDULER)
    public void recoverPublishing() {
        recoveryService.recoverPublishing();
    }
}
