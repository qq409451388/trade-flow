package com.mtx.trade.ingress.dto;

import java.time.LocalDateTime;

/** Pipeline补拉使用的未 ACK 事件。 */
public record EventDeliveryVO(
        Integer contentType,
        Long eventId,
        Integer sourceSystem,
        String thirdEventKey,
        Long messageVersion,
        Long storageId,
        String storageSha256,
        LocalDateTime createTime) {
}
