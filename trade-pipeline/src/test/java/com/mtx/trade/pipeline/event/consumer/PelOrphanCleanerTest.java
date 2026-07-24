package com.mtx.trade.pipeline.event.consumer;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PelOrphanCleanerTest {

    @Test
    void shouldAckOnlyUnclaimedRecordWhoseStreamBodyIsMissing() {
        List<String> acknowledged = new ArrayList<>();
        StringRedisTemplate redisTemplate = redisTemplate(false, acknowledged);
        PelOrphanCleaner cleaner = new PelOrphanCleaner(redisTemplate);
        RecordId missing = RecordId.of("1-0");
        RecordId claimedId = RecordId.of("2-0");

        cleaner.clean("stream", "group", List.of(missing, claimedId), List.of(record(claimedId)));

        assertThat(acknowledged).containsExactly("1-0");
    }

    @Test
    void shouldKeepUnclaimedRecordWhenStreamBodyStillExists() {
        List<String> acknowledged = new ArrayList<>();
        PelOrphanCleaner cleaner = new PelOrphanCleaner(redisTemplate(true, acknowledged));

        cleaner.clean("stream", "group", List.of(RecordId.of("1-0")), List.of());

        assertThat(acknowledged).isEmpty();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static StringRedisTemplate redisTemplate(boolean bodyExists, List<String> acknowledged) {
        StreamOperations<String, Object, Object> operations =
                (StreamOperations<String, Object, Object>) Proxy.newProxyInstance(
                        StreamOperations.class.getClassLoader(),
                        new Class<?>[]{StreamOperations.class},
                        (proxy, method, args) -> {
                            if ("range".equals(method.getName())) {
                                return bodyExists ? List.of(record(RecordId.of("1-0"))) : List.of();
                            }
                            if ("acknowledge".equals(method.getName())) {
                                RecordId[] ids = (RecordId[]) args[2];
                                for (RecordId id : ids) {
                                    acknowledged.add(id.getValue());
                                }
                                return (long) ids.length;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
        return new StringRedisTemplate() {
            @Override
            public <HK, HV> StreamOperations<String, HK, HV> opsForStream() {
                return (StreamOperations) operations;
            }
        };
    }

    private static MapRecord<String, Object, Object> record(RecordId id) {
        return StreamRecords.<String, Object, Object>mapBacked(Map.of("eventId", "1"))
                .withStreamKey("stream")
                .withId(id);
    }
}
