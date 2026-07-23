package com.mtx.trade.pipeline.dto;

import java.time.LocalDateTime;

/** Ingress 自动未 ACK的事件引用。 */
public record IngressUnackedEvent(
        Integer contentType,
        Long eventId,
        Integer sourceSystem,
        String thirdEventKey,
        Long messageVersion,
        Long storageId,
        String storageSha256,
        LocalDateTime createTime) {
}
