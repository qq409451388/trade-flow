package com.mtx.trade.pipeline.dto;

import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;

/** Ingress 发布到支付 Stream 的稳定事件引用。 */
public record PaymentEventMessage(
        long eventId,
        long storageId,
        byte[] storageSha256,
        String eventKey,
        int sourceSystem,
        int contentType,
        long messageVersion) {

    public PaymentEventMessage {
        storageSha256 = storageSha256 == null ? new byte[0] : storageSha256.clone();
    }

    @Override
    public byte[] storageSha256() {
        return storageSha256.clone();
    }

    public static PaymentEventMessage from(Map<Object, Object> fields) {
        try {
            long eventId = positiveLong(fields, "eventId");
            long storageId = positiveLong(fields, "storageId");
            byte[] sha256 = HexFormat.of().parseHex(required(fields, "storageSha256"));
            if (sha256.length != 32) {
                throw new IllegalArgumentException("storageSha256 length must be 32 bytes");
            }
            int contentType = Math.toIntExact(positiveLong(fields, "contentType"));
            if (contentType != ContentType.PAYMENT.getCode()) {
                throw new IllegalArgumentException("not a payment event: " + contentType);
            }
            return new PaymentEventMessage(
                    eventId,
                    storageId,
                    sha256,
                    required(fields, "eventKey"),
                    nonNegativeInt(fields, "sourceSystem"),
                    contentType,
                    nonNegativeLong(fields, "messageVersion"));
        } catch (RuntimeException e) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "支付 Stream 消息无效: " + e.getMessage());
        }
    }

    private static long positiveLong(Map<Object, Object> fields, String name) {
        long value = Long.parseLong(required(fields, name));
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long nonNegativeLong(Map<Object, Object> fields, String name) {
        long value = Long.parseLong(required(fields, name));
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    private static int nonNegativeInt(Map<Object, Object> fields, String name) {
        int value = Integer.parseInt(required(fields, name));
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    private static String required(Map<Object, Object> fields, String name) {
        Object value = fields.get(name);
        if (value == null || value.toString().isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.toString();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof PaymentEventMessage other)) {
            return false;
        }
        return eventId == other.eventId
                && storageId == other.storageId
                && sourceSystem == other.sourceSystem
                && contentType == other.contentType
                && messageVersion == other.messageVersion
                && java.util.Objects.equals(eventKey, other.eventKey)
                && Arrays.equals(storageSha256, other.storageSha256);
    }

    @Override
    public int hashCode() {
        return 31 * java.util.Objects.hash(eventId, storageId, eventKey,
                sourceSystem, contentType, messageVersion) + Arrays.hashCode(storageSha256);
    }
}
