package com.mtx.trade.common.id;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 全局 ID 生成配置。
 *
 * <pre>
 * global-id:
 *   enabled: true
 *   snowflake:
 *     datacenter-id: 1
 *     worker-id: 1
 *     max-clock-backward-ms: 5
 *     epoch: 1704067200000
 *   domains:
 *     - order
 *     - payment
 *     - event
 * </pre>
 */
@ConfigurationProperties(prefix = "global-id")
public class GlobalIdProperties {

    /**
     * 默认 epoch：2024-01-01 00:00:00 UTC
     */
    public static final long DEFAULT_EPOCH = 1704067200000L;

    private boolean enabled = true;

    private SnowflakeProperties snowflake = new SnowflakeProperties();

    private Set<String> domains = new LinkedHashSet<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public SnowflakeProperties getSnowflake() {
        return snowflake;
    }

    public void setSnowflake(SnowflakeProperties snowflake) {
        this.snowflake = snowflake;
    }

    public Set<String> getDomains() {
        return domains;
    }

    public void setDomains(Set<String> domains) {
        this.domains = domains;
    }

    public static class SnowflakeProperties {

        private long datacenterId = 0;
        private long workerId = 0;
        private long maxClockBackwardMs = 5;
        private long epoch = DEFAULT_EPOCH;

        public long getDatacenterId() {
            return datacenterId;
        }

        public void setDatacenterId(long datacenterId) {
            this.datacenterId = datacenterId;
        }

        public long getWorkerId() {
            return workerId;
        }

        public void setWorkerId(long workerId) {
            this.workerId = workerId;
        }

        public long getMaxClockBackwardMs() {
            return maxClockBackwardMs;
        }

        public void setMaxClockBackwardMs(long maxClockBackwardMs) {
            this.maxClockBackwardMs = maxClockBackwardMs;
        }

        public long getEpoch() {
            return epoch;
        }

        public void setEpoch(long epoch) {
            this.epoch = epoch;
        }
    }
}
