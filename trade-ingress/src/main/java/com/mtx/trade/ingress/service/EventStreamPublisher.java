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

/**
 * 事件流发布器。
 *
 * <p>将订单/支付事件发布到 Redis Stream，供 trade-pipeline 消费。
 * 消息体只携带事件引用（eventId、storageId、storageSha256、来源、类型、版本、状态），
 * 不携带 Storage DO 或物理分表信息，符合 storage-design.md 的稳定端口约束。</p>
 *
 * <p>当前阶段为"尽力而为"发布：Redis 故障时只记录 warn 日志，不影响主流程响应。
 * 后期接入 Outbox 后由 Outbox 轮询保证可靠投递。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventStreamPublisher {

    private final StringRedisTemplate stringRedisTemplate;
    private final EventStreamProperties properties;

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

        addWithBoundedLength(properties.getOrderEventKey(), fields);
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

        addWithBoundedLength(properties.getPaymentEventKey(), fields);
        log.debug("[Redis Stream] ✅ Payment event published. stream={}, eventId={}, storageId={}",
                properties.getPaymentEventKey(), eventDO.getId(), eventDO.getRawId());
    }

    private void addWithBoundedLength(String streamKey, Map<String, String> fields) {
        if (properties.getMaxLength() <= 0) {
            throw new IllegalStateException("trade.stream.max-length 必须为正数");
        }
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

    private static byte[] serialize(RedisSerializer<String> serializer, String value) {
        byte[] serialized = serializer == null ? null : serializer.serialize(value);
        return serialized == null ? value.getBytes(StandardCharsets.UTF_8) : serialized;
    }
}
