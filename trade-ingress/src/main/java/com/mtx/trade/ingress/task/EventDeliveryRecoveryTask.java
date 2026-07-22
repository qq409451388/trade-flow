package com.mtx.trade.ingress.task;

import com.mtx.trade.ingress.config.EventDeliveryConfiguration;
import com.mtx.trade.ingress.service.EventDeliveryRecoveryService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 定时执行投递熔断探活、半开探测和积压恢复。 */
@Component
@RequiredArgsConstructor
public class EventDeliveryRecoveryTask {

    private final EventDeliveryRecoveryService recoveryService;

    @Scheduled(
            fixedDelayString = "${trade.ingress.event-delivery.circuit.scheduler-delay-ms:30000}",
            scheduler = EventDeliveryConfiguration.CIRCUIT_RECOVERY_SCHEDULER)
    public void recoverCircuitsAndDrainBacklog() {
        recoveryService.recoverCircuitsAndDrainBacklog();
    }
}
