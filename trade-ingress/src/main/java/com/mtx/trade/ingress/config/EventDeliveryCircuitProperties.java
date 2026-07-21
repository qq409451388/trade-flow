package com.mtx.trade.ingress.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Ingress Redis Stream 投递熔断配置。 */
@Data
@ConfigurationProperties("trade.ingress.event-delivery.circuit")
public class EventDeliveryCircuitProperties {

    private boolean enabled = true;
    private Duration failureWindow = Duration.ofMinutes(1);
    private int failureThreshold = 10;
    private Duration healthCheckDelay = Duration.ofSeconds(30);
    private int healthSuccessThreshold = 2;
    private int halfOpenPermits = 10;
    private Duration recoveryLease = Duration.ofMinutes(2);
    private Duration stateCacheTtl = Duration.ofSeconds(5);
    private int recoveryBatchSize = 100;
    private String pipelineReadinessUrl =
            "http://127.0.0.1:8083/trade-pipeline/readiness/event-consumer";
    private Duration readinessConnectTimeout = Duration.ofSeconds(3);
    private Duration readinessReadTimeout = Duration.ofSeconds(5);
}
