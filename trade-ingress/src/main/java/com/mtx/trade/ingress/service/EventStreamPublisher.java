package com.mtx.trade.ingress.service;

import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.ingress.config.EventStreamProperties;
import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.entity.PaymentEventDO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事件流发布器。
 *
 * <p>将订单/支付事件发布到 Redis Stream，供 trade-pipeline 消费。
 * 消息体只携带事件引用（eventId、storageId、storageSha256、来源、类型、版本、状态），
 * 不携带 Storage DO 或物理分表信息，符合 storage-design.md 的稳定端口约束。</p>
 *
 * <p>Redis 只承担受保护的实时通知。发布失败不影响已经持久化的 event，历史遗漏由
 * Pipeline 定时补拉 MySQL 中的未 ACK event 收敛。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventStreamPublisher {

    private final StringRedisTemplate stringRedisTemplate;
    private final EventStreamProperties properties;
    private final Map<String, Boolean> pausedStreams = new ConcurrentHashMap<>();
    private long rateWindowSecond;
    private int rateWindowCount;

    /**
     * 发布订单事件到 Redis Stream。
     *
     * @param eventDO 已落库的订单事件
     */
    public void publishOrderEvent(OrderEventDO eventDO) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("eventId", String.valueOf(eventDO.getId()));
        fields.put("storageId", String.valueOf(eventDO.getRawId()));
        fields.put("storageSha256", HexFormat.of().formatHex(eventDO.getPayloadSha256()));
        fields.put("eventKey", eventDO.getThirdEventKey());
        fields.put("sourceSystem", String.valueOf(eventDO.getSourceSystem()));
        fields.put("contentType", String.valueOf(ContentType.ORDER.getCode()));
        fields.put("messageVersion", String.valueOf(eventDO.getMessageVersion()));

        addWithBoundedLength(properties.getOrderEventKey(), properties.getOrderConsumerGroup(), fields);
        // 成功发布的debug级别
        log.debug("[Redis Stream] ✅ Order event published. stream={}, eventId={}, storageId={}",
                properties.getOrderEventKey(), eventDO.getId(), eventDO.getRawId());
    }

    public void publishPaymentEvent(PaymentEventDO eventDO) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("eventId", String.valueOf(eventDO.getId()));
        fields.put("storageId", String.valueOf(eventDO.getRawId()));
        fields.put("storageSha256", HexFormat.of().formatHex(eventDO.getPayloadSha256()));
        fields.put("eventKey", eventDO.getThirdEventKey());
        fields.put("sourceSystem", String.valueOf(eventDO.getSourceSystem()));
        fields.put("contentType", String.valueOf(ContentType.PAYMENT.getCode()));
        fields.put("messageVersion", String.valueOf(eventDO.getMessageVersion()));

        addWithBoundedLength(properties.getPaymentEventKey(), properties.getPaymentConsumerGroup(), fields);
        log.debug("[Redis Stream] ✅ Payment event published. stream={}, eventId={}, storageId={}",
                properties.getPaymentEventKey(), eventDO.getId(), eventDO.getRawId());
    }

    /** Redis、目标消费组和低水位均可确认时，才允许 OPEN 状态恢复实时发布。 */
    public boolean readyToResume(int contentType) {
        if (contentType == ContentType.ORDER.getCode()) {
            return readyToResume(properties.getOrderEventKey(), properties.getOrderConsumerGroup());
        }
        if (contentType == ContentType.PAYMENT.getCode()) {
            return readyToResume(properties.getPaymentEventKey(), properties.getPaymentConsumerGroup());
        }
        return false;
    }

    private void addWithBoundedLength(String streamKey, String consumerGroup, Map<String, String> fields) {
        if (properties.getMaxLength() <= 0) {
            throw new IllegalStateException("trade.stream.max-length 必须为正数");
        }
        enforcePublishProtection(streamKey, consumerGroup);
        RedisSerializer<String> serializer = stringRedisTemplate.getStringSerializer();
        RecordId recordId = stringRedisTemplate.execute((RedisCallback<RecordId>) connection -> {
            Map<byte[], byte[]> rawFields = new LinkedHashMap<>();
            fields.forEach((key, value) -> rawFields.put(serialize(serializer, key), serialize(serializer, value)));
            ByteRecord record = StreamRecords.rawBytes(rawFields)
                    .withStreamKey(serialize(serializer, streamKey));
            return connection.streamCommands().xAdd(record,
                    XAddOptions.maxlen(properties.getMaxLength()).approximateTrimming(true));
        });
        if (recordId == null) {
            throw new IllegalStateException("Redis Stream XADD 未返回 recordId");
        }
    }

    private void enforcePublishProtection(String streamKey, String consumerGroup) {
        boolean groupReady;
        try {
            groupReady = stringRedisTemplate.opsForStream().groups(streamKey).stream()
                    .anyMatch(group -> consumerGroup.equals(group.groupName()));
        } catch (RuntimeException e) {
            throw new IllegalStateException("Redis consumer group status is unavailable", e);
        }
        if (!groupReady) {
            throw new IllegalStateException("Redis consumer group does not exist: " + consumerGroup);
        }
        Long size = stringRedisTemplate.opsForStream().size(streamKey);
        long high = properties.getHighWatermark();
        long low = properties.getLowWatermark();
        if (high <= 0 || low < 0 || low >= high || high > properties.getMaxLength()) {
            throw new IllegalStateException("trade.stream水位配置无效");
        }
        if (Boolean.TRUE.equals(pausedStreams.get(streamKey))) {
            if (size != null && size <= low) {
                pausedStreams.remove(streamKey);
                log.info("[Redis Stream] ✅ Stream dropped below low watermark; publishing resumed. stream={}, size={}",
                        streamKey, size);
            } else {
                throw new IllegalStateException("Redis Stream publishing paused at high watermark");
            }
        }
        if (size != null && size >= high) {
            pausedStreams.put(streamKey, true);
            log.warn("[Redis Stream] 🔄 Stream reached high watermark; publishing paused and MySQL remains "
                    + "authoritative. stream={}, size={}, highWatermark={}", streamKey, size, high);
            throw new IllegalStateException("Redis Stream reached high watermark");
        }
        acquireRatePermit();
    }

    private boolean readyToResume(String streamKey, String consumerGroup) {
        try {
            boolean groupReady = stringRedisTemplate.opsForStream().groups(streamKey).stream()
                    .anyMatch(group -> consumerGroup.equals(group.groupName()));
            Long size = stringRedisTemplate.opsForStream().size(streamKey);
            long low = properties.getLowWatermark();
            return groupReady && size != null && low >= 0 && size <= low;
        } catch (RuntimeException e) {
            log.debug("[Redis Stream] Redis publish readiness is unavailable. stream={}", streamKey, e);
            return false;
        }
    }

    private synchronized void acquireRatePermit() {
        int limit = properties.getPublishRatePerSecond();
        if (limit <= 0) {
            throw new IllegalStateException("trade.stream.publish-rate-per-second必须为正数");
        }
        long second = System.currentTimeMillis() / 1000;
        if (second != rateWindowSecond) {
            rateWindowSecond = second;
            rateWindowCount = 0;
        }
        if (++rateWindowCount > limit) {
            throw new IllegalStateException("Redis Stream publish rate limit exceeded");
        }
    }

    private static byte[] serialize(RedisSerializer<String> serializer, String value) {
        byte[] serialized = serializer == null ? null : serializer.serialize(value);
        return serialized == null ? value.getBytes(StandardCharsets.UTF_8) : serialized;
    }
}
