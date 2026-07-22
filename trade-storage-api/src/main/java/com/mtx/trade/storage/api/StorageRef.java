package com.mtx.trade.storage.api;

/**
 * Storage 写入结果，可安全放入后续任务或消息中。
 */
public record StorageRef(Long storageId, byte[] sha256, int contentLength) {

    public StorageRef {
        if (storageId == null || storageId <= 0) {
            throw new IllegalArgumentException("storageId 必须为正数");
        }
        if (sha256 == null || sha256.length != 32) {
            throw new IllegalArgumentException("storage sha256 必须为32字节");
        }
        if (contentLength < 0) {
            throw new IllegalArgumentException("contentLength 不能为负数");
        }
        sha256 = sha256.clone();
    }

    @Override
    public byte[] sha256() {
        return sha256.clone();
    }

    public StorageKey key() {
        return new StorageKey(storageId, sha256);
    }
}
