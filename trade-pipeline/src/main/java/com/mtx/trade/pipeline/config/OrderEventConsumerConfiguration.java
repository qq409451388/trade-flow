package com.mtx.trade.pipeline.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** 注册订单事件消费及富友报文配置。 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({OrderEventConsumerProperties.class, FuiouOrderProperties.class})
public class OrderEventConsumerConfiguration {
}
