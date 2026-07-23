package com.mtx.trade.ingress.config;

import com.mtx.trade.ingress.constants.RedisKeyConstants;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Redis Stream 名称与内存保留上限。 */
@Data
@ConfigurationProperties(prefix = "trade.stream")
public class EventStreamProperties {

    private String orderEventKey = RedisKeyConstants.ORDER_EVENT_STREAM;
    private String paymentEventKey = RedisKeyConstants.PAYMENT_EVENT_STREAM;
    private String orderConsumerGroup = "trade-pipeline-order";
    private String paymentConsumerGroup = "trade-pipeline-payment";
    private long maxLength = 1_000_000L;
    private long highWatermark = 900_000L;
    private long lowWatermark = 700_000L;
    private int publishRatePerSecond = 2_000;
}
