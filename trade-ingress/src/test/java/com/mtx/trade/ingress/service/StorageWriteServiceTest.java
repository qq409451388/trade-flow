package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.constants.RedisKeyConstants;
import com.mtx.trade.ingress.config.StorageWriteProperties;
import com.mtx.trade.ingress.utils.RedisLock;
import com.mtx.trade.storage.api.StorageRef;
import com.mtx.trade.storage.api.StorageWriteCommand;
import com.mtx.trade.storage.api.StorageWriteException;
import com.mtx.trade.storage.api.StorageWriter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StorageWriteServiceTest {

    @Test
    void shouldLockBySourceSystemAndFullSha256BeforeWriting() {
        List<String> events = new ArrayList<>();
        RecordingRedisLock redisLock = new RecordingRedisLock(events);
        StorageRef expected = new StorageRef(10L, new byte[32], 3);
        StorageWriter storageWriter = command -> {
            events.add("write");
            return expected;
        };
        StorageWriteProperties properties = properties();
        StorageWriteService service = new StorageWriteService(storageWriter, redisLock, properties);

        StorageRef actual = service.putIfAbsent(
                new StorageWriteCommand(1, 1, new byte[]{1, 2, 3}, null));

        String expectedKey = RedisKeyConstants.STORAGE_PUT_IF_ABSENT_LOCK + "1:"
                + "039058c6f2c0cb492c533b0a4d14ef77cc0f78abccced5287d84a1a2011cfb81";
        assertThat(actual).isSameAs(expected);
        assertThat(redisLock.lockKey).isEqualTo(expectedKey);
        assertThat(redisLock.waitTime).isEqualTo(Duration.ofSeconds(5));
        assertThat(redisLock.leaseTime).isEqualTo(Duration.ofMinutes(11));
        assertThat(events).containsExactly("lock", "write", "release");
    }

    @Test
    void shouldReleaseLockWhenStorageWriteFails() {
        List<String> events = new ArrayList<>();
        RecordingRedisLock redisLock = new RecordingRedisLock(events);
        StorageWriter storageWriter = command -> {
            events.add("write");
            throw new StorageWriteException("failed");
        };
        StorageWriteService service = new StorageWriteService(storageWriter, redisLock, properties());

        assertThatThrownBy(() -> service.putIfAbsent(
                new StorageWriteCommand(1, 1, new byte[]{1}, null)))
                .isInstanceOf(StorageWriteException.class);

        assertThat(events).containsExactly("lock", "write", "release");
    }

    private static StorageWriteProperties properties() {
        StorageWriteProperties properties = new StorageWriteProperties();
        properties.setLockWait(Duration.ofSeconds(5));
        properties.setLockLease(Duration.ofMinutes(11));
        return properties;
    }

    private static final class RecordingRedisLock extends RedisLock {
        private final List<String> events;
        private String lockKey;
        private Duration waitTime;
        private Duration leaseTime;

        private RecordingRedisLock(List<String> events) {
            super(null);
            this.events = events;
        }

        @Override
        public boolean acquireLock(String lockKey, Duration waitTime, Duration leaseTime) {
            this.lockKey = lockKey;
            this.waitTime = waitTime;
            this.leaseTime = leaseTime;
            events.add("lock");
            return true;
        }

        @Override
        public boolean releaseLock(String lockKey) {
            events.add("release");
            return true;
        }
    }
}
