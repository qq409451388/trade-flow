package com.mtx.trade.ingress.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Ingress 请求链路可观测性配置。 */
@ConfigurationProperties("trade.ingress.observability")
public class IngressObservabilityProperties {

    private Duration slowRequestThreshold = Duration.ofSeconds(1);

    public Duration getSlowRequestThreshold() {
        return slowRequestThreshold;
    }

    public void setSlowRequestThreshold(Duration slowRequestThreshold) {
        this.slowRequestThreshold = slowRequestThreshold;
    }
}
