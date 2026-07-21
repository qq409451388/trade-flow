package com.mtx.trade.ingress.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Redis分布式锁工具。
 *
 * @author codex
 */
@Slf4j
@Component
public class RedisLock {
    private static final String SUFFIX = "v2";
    private static final Duration LOCK_WAIT_INTERVAL = Duration.ofMillis(100);
    private static final DefaultRedisScript<Long> RELEASE_LOCK_SCRIPT = new DefaultRedisScript<>("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);
    private static final ThreadLocal<Map<String, String>> LOCK_VALUES = ThreadLocal.withInitial(HashMap::new);

    private final StringRedisTemplate stringRedisTemplate;

    public RedisLock(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 阻塞等待直到获取锁或超时；成功返回true，失败抛出RuntimeException。
     *
     * @author codex
     */
    public boolean acquireLock(String lockKey, long waitMillis) {
        String actualKey = getLockKey(lockKey);
        String lockValue = UUID.randomUUID().toString();
        long deadline = System.currentTimeMillis() + Math.max(waitMillis, 0L);
        try {
            do {
                Boolean locked = stringRedisTemplate.opsForValue()
                        .setIfAbsent(actualKey, lockValue, Duration.ofMillis(8000));
                if (Boolean.TRUE.equals(locked)) {
                    LOCK_VALUES.get().put(actualKey, lockValue);
                    return true;
                }
                sleepForLockWait();
            } while (System.currentTimeMillis() < deadline);
        } catch (Exception exception) {
            log.error("RedisLock error while acquiring key:{}", actualKey, exception);
        }
        throw new RuntimeException("Failed to acquire Redis lock in the program.");
    }

    /**
     * 立即尝试获取锁。
     *
     * @author codex
     */
    public boolean acquireInstantLock(String lockKey, Integer leaseTimeNum, TimeUnit timeUnit) {
        String actualKey = getLockKey(lockKey);
        String lockValue = UUID.randomUUID().toString();
        try {
            long leaseMillis = timeUnit.toMillis(leaseTimeNum.longValue());
            Boolean locked = stringRedisTemplate.opsForValue()
                    .setIfAbsent(actualKey, lockValue, Duration.ofMillis(leaseMillis));
            if (Boolean.TRUE.equals(locked)) {
                LOCK_VALUES.get().put(actualKey, lockValue);
                return true;
            }
            return false;
        } catch (Exception exception) {
            log.error("Fallback setIfAbsent failed for key:{}", actualKey, exception);
            return false;
        }
    }

    /**
     * 仅当当前线程持有锁时释放。
     *
     * @author codex
     */
    public boolean releaseLock(String lockKey) {
        String actualKey = getLockKey(lockKey);
        String lockValue = LOCK_VALUES.get().remove(actualKey);
        if (lockValue == null) {
            return false;
        }
        try {
            Long released = stringRedisTemplate.execute(RELEASE_LOCK_SCRIPT, Collections.singletonList(actualKey), lockValue);
            if (released != null && released > 0) {
                return true;
            }
            return false;
        } catch (Exception exception) {
            log.error("releaseLock error, key:{}", actualKey, exception);
            return false;
        } finally {
            if (LOCK_VALUES.get().isEmpty()) {
                LOCK_VALUES.remove();
            }
        }
    }

    private String getLockKey(String key) {
        return key + "_" + SUFFIX;
    }

    private void sleepForLockWait() {
        try {
            Thread.sleep(LOCK_WAIT_INTERVAL.toMillis());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Redis lock wait interrupted.", exception);
        }
    }
}
