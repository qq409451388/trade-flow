package com.mtx.trade.ingress.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/** Ingress Redis实时通知配置。 */
@ConfigurationProperties("trade.ingress.event-delivery")
public class EventDeliveryProperties {

    private List<Duration> retryDelays = List.of(
            Duration.ofMinutes(1), Duration.ofMinutes(2));
    /** Redis 实时链路优先处理窗口；窗口外的未 ACK 事件可由 Pipeline 补拉。 */
    private Duration realtimeGracePeriod = Duration.ofSeconds(30);

    public List<Duration> getRetryDelays() {
        return retryDelays;
    }

    public void setRetryDelays(List<Duration> retryDelays) {
        this.retryDelays = retryDelays;
    }

    public Duration getRealtimeGracePeriod() {
        return realtimeGracePeriod;
    }

    public void setRealtimeGracePeriod(Duration realtimeGracePeriod) {
        this.realtimeGracePeriod = realtimeGracePeriod;
    }

}
