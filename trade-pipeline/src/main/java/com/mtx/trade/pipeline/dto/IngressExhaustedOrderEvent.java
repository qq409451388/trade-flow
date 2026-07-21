package com.mtx.trade.pipeline.dto;

import java.time.LocalDateTime;

/** Ingress 自动投递耗尽的订单事件引用。 */
public record IngressExhaustedOrderEvent(
        Integer contentType,
        Long eventId,
        Integer sourceSystem,
        String thirdEventKey,
        Long messageVersion,
        Long storageId,
        String storageSha256,
        Integer autoRedeliveryCount,
        LocalDateTime lastRedeliveryTime,
        LocalDateTime createTime) {
}
