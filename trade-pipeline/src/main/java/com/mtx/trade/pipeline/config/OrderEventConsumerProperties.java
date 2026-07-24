package com.mtx.trade.pipeline.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** 订单事件 Redis Stream 消费配置。 */
@Data
@ConfigurationProperties(prefix = "trade.pipeline.order-event-consumer")
public class OrderEventConsumerProperties {

    private boolean enabled = true;
    private String streamKey = "stream:order-event";
    private String group = "trade-pipeline-order";
    private String consumerName = "pipeline-order-1";
    private int batchSize = 100;
    private int workerCount = 8;
    private int workerQueueCapacity = 250;
    private Duration blockTimeout = Duration.ofSeconds(2);
    private Duration reclaimDelay = Duration.ofSeconds(30);
    private Duration pendingMinIdle = Duration.ofMinutes(1);
    private String ingressAckUrl = "http://127.0.0.1:8115/trade-ingress/event/ack";
    private String ingressBatchAckUrl = "http://127.0.0.1:8115/trade-ingress/event/batch-ack";
    private Duration ingressAckConnectTimeout = Duration.ofSeconds(3);
    private Duration ingressAckReadTimeout = Duration.ofSeconds(5);
    private String ingressUnackedUrl = "http://127.0.0.1:8115/trade-ingress/event/unacked";
}
