package com.mtx.trade.storage.api;

/**
 * Storage 稳定定位键。ID 用于分片内主键定位，SHA-256 用于物理分片路由。
 */
public record StorageKey(Long storageId, byte[] sha256) {

    public StorageKey {
        if (storageId == null || storageId <= 0) {
            throw new IllegalArgumentException("storageId 必须为正数");
        }
        if (sha256 == null || sha256.length != 32) {
            throw new IllegalArgumentException("storage sha256 必须为32字节");
        }
        sha256 = sha256.clone();
    }

    @Override
    public byte[] sha256() {
        return sha256.clone();
    }
}
