package com.mtx.trade.pipeline.event.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** 清理已被 Stream 裁剪、但仍残留在消费组 PEL 中的孤儿引用。 */
@Slf4j
@Service
@RequiredArgsConstructor
class PelOrphanCleaner {

    private final StringRedisTemplate redisTemplate;

    public void clean(
            String streamKey,
            String group,
            List<RecordId> attemptedIds,
            List<MapRecord<String, Object, Object>> claimedRecords) {
        if (attemptedIds == null || attemptedIds.isEmpty()) {
            return;
        }
        Set<String> claimedIds = new HashSet<>();
        if (claimedRecords != null) {
            claimedRecords.forEach(record -> claimedIds.add(record.getId().getValue()));
        }
        for (RecordId attemptedId : attemptedIds) {
            if (claimedIds.contains(attemptedId.getValue())) {
                continue;
            }
            try {
                var existing = redisTemplate.opsForStream().range(
                        streamKey,
                        Range.closed(attemptedId.getValue(), attemptedId.getValue()),
                        Limit.limit().count(1));
                if (existing != null && !existing.isEmpty()) {
                    // 记录仍存在，通常是刚被其他实例接管并刷新了 idle，不能误清理。
                    continue;
                }
                Long acknowledged = redisTemplate.opsForStream().acknowledge(streamKey, group, attemptedId);
                if (acknowledged != null && acknowledged > 0) {
                    log.warn("[PEL Recovery] ✅ Trimmed Stream orphan was removed from PEL. "
                                    + "stream={}, group={}, recordId={}",
                            streamKey, group, attemptedId.getValue());
                }
            } catch (RuntimeException e) {
                log.warn("[PEL Recovery] 🔄 PEL orphan check failed; pending reference was retained. "
                                + "stream={}, group={}, recordId={}",
                        streamKey, group, attemptedId.getValue(), e);
            }
        }
    }
}
