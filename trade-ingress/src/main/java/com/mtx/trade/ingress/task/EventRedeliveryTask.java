package com.mtx.trade.ingress.task;

import com.mtx.trade.ingress.config.EventDeliveryConfiguration;
import com.mtx.trade.ingress.service.EventDeliveryService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 定时扫描并补发长时间未确认的 Ingress 事件。 */
@Component
@RequiredArgsConstructor
public class EventRedeliveryTask {

    private final EventDeliveryService eventDeliveryService;

    @Scheduled(
            cron = "${trade.ingress.event-delivery.scan-cron:0 */15 * * * *}",
            scheduler = EventDeliveryConfiguration.EVENT_REDELIVERY_SCHEDULER)
    public void redeliverStaleUnacknowledgedEvents() {
        eventDeliveryService.redeliverStaleUnackedEvents();
    }
}
