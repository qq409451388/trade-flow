package com.mtx.trade.pipeline.task;

import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.pipeline.config.EventConsumerConfiguration;
import com.mtx.trade.pipeline.service.EventStreamListenerRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 独立守护订单、支付实时 Stream 监听任务，避免监听线程退出后长期无人发现。 */
@Component
@RequiredArgsConstructor
public class EventStreamListenerWatchdogTask {

    private final EventStreamListenerRegistry listenerRegistry;

    @Scheduled(
            initialDelayString = "${trade.pipeline.order-event-consumer.listener-watchdog-delay:5000}",
            fixedDelayString = "${trade.pipeline.order-event-consumer.listener-watchdog-delay:5000}",
            scheduler = EventConsumerConfiguration.STREAM_WATCHDOG_SCHEDULER)
    public void ensureOrderListenerActive() {
        listenerRegistry.ensureActive(ContentType.ORDER.getCode());
    }

    @Scheduled(
            initialDelayString = "${trade.pipeline.payment-event-consumer.listener-watchdog-delay:5000}",
            fixedDelayString = "${trade.pipeline.payment-event-consumer.listener-watchdog-delay:5000}",
            scheduler = EventConsumerConfiguration.STREAM_WATCHDOG_SCHEDULER)
    public void ensurePaymentListenerActive() {
        listenerRegistry.ensureActive(ContentType.PAYMENT.getCode());
    }
}
