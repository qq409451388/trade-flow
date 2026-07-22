package com.mtx.trade.pipeline.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** 注册订单、支付事件消费及富友报文配置。 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({
        OrderEventConsumerProperties.class,
        PaymentEventConsumerProperties.class,
        ExhaustedEventPullProperties.class,
        FuiouOrderProperties.class,
        FuiouPaymentProperties.class
})
public class EventConsumerConfiguration {
}
