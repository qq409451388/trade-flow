package com.mtx.trade.pipeline.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Ingress 投递耗尽事件自动拉取配置。 */
@Data
@ConfigurationProperties(prefix = "trade.pipeline.exhausted-event-pull")
public class ExhaustedEventPullProperties {

    private boolean enabled = true;
    private long initialDelayMs = 60_000L;
    private long fixedDelayMs = 60_000L;
    private int batchSize = 100;
    private Duration leaseDuration = Duration.ofMinutes(15);
}
