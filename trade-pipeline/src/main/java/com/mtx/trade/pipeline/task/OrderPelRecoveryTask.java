package com.mtx.trade.pipeline.task;

import com.mtx.trade.pipeline.config.EventConsumerConfiguration;
import com.mtx.trade.pipeline.event.consumer.OrderEventStreamConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 定时接管订单消费组中长时间未确认的 PEL 消息。 */
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "trade.pipeline.order-event-consumer",
        name = "enabled", havingValue = "true", matchIfMissing = true)
public class OrderPelRecoveryTask {

    private final OrderEventStreamConsumer consumer;

    @Scheduled(
            fixedDelayString = "${trade.pipeline.order-event-consumer.reclaim-delay:30000}",
            scheduler = EventConsumerConfiguration.ORDER_PEL_SCHEDULER)
    public void reclaimPendingMessages() {
        consumer.reclaimPendingMessages();
    }
}
