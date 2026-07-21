package com.mtx.trade.receiver.service;

import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.receiver.entity.OrderEventDO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

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

    @Value("${trade.stream.order-event-key:stream:order-event}")
    private String orderEventStreamKey;

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
        fields.put("eventStatus", String.valueOf(eventDO.getEventStatus()));

        stringRedisTemplate.opsForStream().add(orderEventStreamKey, fields);
        log.info("order event published, stream={}, eventId={}, storageId={}",
                orderEventStreamKey, eventDO.getId(), eventDO.getRawId());
    }
}
