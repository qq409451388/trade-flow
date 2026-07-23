package com.mtx.trade.pipeline.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Ingress 投递未 ACK 事件自动拉取配置。 */
@Data
@ConfigurationProperties(prefix = "trade.pipeline.unacked-event-pull")
public class UnackedEventPullProperties {

    private boolean enabled = true;
    private long initialDelayMs = 60_000L;
    private long fixedDelayMs = 60_000L;
    private int batchSize = 500;
    private int maxBatchesPerRun = 100;
    private int parallelism = 4;
    private int terminalFailureAttempts = 3;
    private Duration maxRunDuration = Duration.ofMinutes(10);
    private Duration leaseDuration = Duration.ofMinutes(15);
}
