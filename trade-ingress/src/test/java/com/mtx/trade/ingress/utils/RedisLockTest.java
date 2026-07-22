package com.mtx.trade.ingress.utils;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisLockTest {

    @Test
    void shouldReturnFalseWhenLockIsAlreadyHeld() {
        StringRedisTemplate redisTemplate = redisTemplateReturning(false);
        RedisLock redisLock = new RedisLock(redisTemplate);

        assertThat(redisLock.acquireInstantLock("job", 1, TimeUnit.SECONDS)).isFalse();
    }

    @Test
    void shouldPropagateRedisFailureInsteadOfTreatingItAsLockContention() {
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
        when(valueOperations.setIfAbsent(anyString(), anyString(), any(Duration.class)))
                .thenThrow(new IllegalStateException("redis unavailable"));
        RedisLock redisLock = new RedisLock(redisTemplate(valueOperations));

        assertThatThrownBy(() -> redisLock.acquireInstantLock("job", 1, TimeUnit.SECONDS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Redis unavailable while acquiring lock");
    }

    private static StringRedisTemplate redisTemplateReturning(boolean result) {
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
        when(valueOperations.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(result);
        return redisTemplate(valueOperations);
    }

    private static StringRedisTemplate redisTemplate(ValueOperations<String, String> valueOperations) {
        return new StringRedisTemplate() {
            @Override
            public ValueOperations<String, String> opsForValue() {
                return valueOperations;
            }
        };
    }
}
