package com.mtx.trade.common.id;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 全局 ID 生成配置。
 *
 * <pre>
 * global-id:
 *   enabled: true
 *   snowflake:
 *     datacenter-id: 1
 *     worker-id: 1
 *     max-clock-backward-ms: 1000
 *     epoch: 1704067200000
 *   domains:
 *     storage:
 *       independent: true
 *       datacenter-id: 2
 *       worker-id: 1
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

    private Map<String, DomainProperties> domains = new LinkedHashMap<>();

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

    public Map<String, DomainProperties> getDomains() {
        return domains;
    }

    public void setDomains(Map<String, DomainProperties> domains) {
        this.domains = domains;
    }

    public static class DomainProperties {

        /**
         * false 时仅预注册领域门面，并继续共享全局引擎。
         */
        private boolean independent = false;
        private Long datacenterId;
        private Long workerId;

        public boolean isIndependent() {
            return independent;
        }

        public void setIndependent(boolean independent) {
            this.independent = independent;
        }

        public Long getDatacenterId() {
            return datacenterId;
        }

        public void setDatacenterId(Long datacenterId) {
            this.datacenterId = datacenterId;
        }

        public Long getWorkerId() {
            return workerId;
        }

        public void setWorkerId(Long workerId) {
            this.workerId = workerId;
        }
    }

    public static class SnowflakeProperties {

        private long datacenterId = 0;
        private long workerId = 0;
        private long maxClockBackwardMs = 1000;
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
