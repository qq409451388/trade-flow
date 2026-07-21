package com.mtx.trade.storage.api;

/**
 * Storage 写入结果，可安全放入后续任务或消息中。
 */
public record StorageRef(Long storageId, byte[] sha256, int contentLength) {

    public StorageRef {
        sha256 = sha256 == null ? new byte[0] : sha256.clone();
    }

    @Override
    public byte[] sha256() {
        return sha256.clone();
    }
}
