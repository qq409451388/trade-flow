package com.mtx.trade.ingress.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Ingress 写入 Storage 时的 Redis 串行化锁配置。 */
@ConfigurationProperties("trade.ingress.storage-write")
public class StorageWriteProperties {

    private Duration lockWait = Duration.ofSeconds(5);
    private Duration lockLease = Duration.ofMinutes(11);

    public Duration getLockWait() {
        return lockWait;
    }

    public void setLockWait(Duration lockWait) {
        this.lockWait = lockWait;
    }

    public Duration getLockLease() {
        return lockLease;
    }

    public void setLockLease(Duration lockLease) {
        this.lockLease = lockLease;
    }
}
