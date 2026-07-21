package com.mtx.trade.pipeline.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.ZoneId;

/** 富友订单报文转换配置。 */
@Data
@ConfigurationProperties(prefix = "trade.pipeline.fuiou-order")
public class FuiouOrderProperties {

    private ZoneId zoneId = ZoneId.of("Asia/Shanghai");
}
