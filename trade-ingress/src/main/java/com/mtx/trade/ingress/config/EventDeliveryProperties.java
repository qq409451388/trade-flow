package com.mtx.trade.ingress.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/** Ingress 事件投递与补发配置。 */
@ConfigurationProperties("trade.ingress.event-delivery")
public class EventDeliveryProperties {

    private List<Duration> retryDelays = List.of(
            Duration.ofMinutes(1), Duration.ofMinutes(5), Duration.ofMinutes(10));
    private Duration staleAfter = Duration.ofMinutes(15);
    private int batchSize = 500;
    private int maxBatches = 20;
    private int maxAutoRedeliveries = 2;
    private Duration scanLockLease = Duration.ofMinutes(14);

    public List<Duration> getRetryDelays() {
        return retryDelays;
    }

    public void setRetryDelays(List<Duration> retryDelays) {
        this.retryDelays = retryDelays;
    }

    public Duration getStaleAfter() {
        return staleAfter;
    }

    public void setStaleAfter(Duration staleAfter) {
        this.staleAfter = staleAfter;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getMaxBatches() {
        return maxBatches;
    }

    public void setMaxBatches(int maxBatches) {
        this.maxBatches = maxBatches;
    }

    public int getMaxAutoRedeliveries() {
        return maxAutoRedeliveries;
    }

    public void setMaxAutoRedeliveries(int maxAutoRedeliveries) {
        this.maxAutoRedeliveries = maxAutoRedeliveries;
    }

    public Duration getScanLockLease() {
        return scanLockLease;
    }

    public void setScanLockLease(Duration scanLockLease) {
        this.scanLockLease = scanLockLease;
    }
}
