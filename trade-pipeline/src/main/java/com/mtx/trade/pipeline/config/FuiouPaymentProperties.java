package com.mtx.trade.pipeline.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.time.ZoneId;

/** 富友支付报文和跨年退款路由配置。 */
@Data
@ConfigurationProperties(prefix = "trade.pipeline.fuiou-payment")
public class FuiouPaymentProperties {

    private ZoneId zoneId = ZoneId.of("Asia/Shanghai");
    private Duration yearBoundaryWindow = Duration.ofHours(3);
}
