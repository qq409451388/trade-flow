package com.mtx.trade.storage.api;

import java.time.LocalDateTime;

/**
 * 对业务暴露的 Storage 元数据，不包含持久化实体和物理分片信息。
 */
public record StorageMetadata(
        Long storageId,
        int sourceSystem,
        int contentType,
        byte[] sha256,
        int payloadLength,
        LocalDateTime receivedTime) {

    public StorageMetadata {
        if (storageId == null || storageId <= 0) {
            throw new IllegalArgumentException("storageId 必须为正数");
        }
        if (sha256 == null || sha256.length != 32) {
            throw new IllegalArgumentException("storage sha256 必须为32字节");
        }
        if (payloadLength < 0) {
            throw new IllegalArgumentException("payloadLength 不能为负数");
        }
        sha256 = sha256.clone();
    }

    @Override
    public byte[] sha256() {
        return sha256.clone();
    }
}
