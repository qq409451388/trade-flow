package com.mtx.trade.ingress.dto;

import java.time.LocalDateTime;

/** 自动补发已耗尽的事件。 */
public record EventDeliveryVO(
        Integer contentType,
        Long eventId,
        Integer sourceSystem,
        String thirdEventKey,
        Long messageVersion,
        Long storageId,
        Integer autoRedeliveryCount,
        LocalDateTime lastRedeliveryTime,
        LocalDateTime createTime) {
}
