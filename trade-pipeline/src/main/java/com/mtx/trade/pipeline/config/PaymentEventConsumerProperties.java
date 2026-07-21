package com.mtx.trade.pipeline.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** 支付事件 Redis Stream 消费配置。 */
@Data
@ConfigurationProperties(prefix = "trade.pipeline.payment-event-consumer")
public class PaymentEventConsumerProperties {

    private boolean enabled = true;
    private String streamKey = "stream:payment-event";
    private String group = "trade-pipeline-payment";
    private String consumerName = "pipeline-payment-1";
    private int batchSize = 100;
    private Duration blockTimeout = Duration.ofSeconds(2);
    private Duration pendingMinIdle = Duration.ofMinutes(1);
}
