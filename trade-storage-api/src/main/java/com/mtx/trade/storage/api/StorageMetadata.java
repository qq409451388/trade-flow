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
        sha256 = sha256 == null ? new byte[0] : sha256.clone();
    }

    @Override
    public byte[] sha256() {
        return sha256.clone();
    }
}
