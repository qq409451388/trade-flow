package com.mtx.trade.pipeline.task;

import com.mtx.trade.pipeline.config.EventConsumerConfiguration;
import com.mtx.trade.pipeline.service.PaymentEventStreamConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 定时接管支付消费组中长时间未确认的 PEL 消息。 */
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "trade.pipeline.payment-event-consumer",
        name = "enabled", havingValue = "true", matchIfMissing = true)
public class PaymentPelRecoveryTask {

    private final PaymentEventStreamConsumer consumer;

    @Scheduled(
            fixedDelayString = "${trade.pipeline.payment-event-consumer.reclaim-delay:30000}",
            scheduler = EventConsumerConfiguration.PAYMENT_PEL_SCHEDULER)
    public void reclaimPendingMessages() {
        consumer.reclaimPendingMessages();
    }
}
