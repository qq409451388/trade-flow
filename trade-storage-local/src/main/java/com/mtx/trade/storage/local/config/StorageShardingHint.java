package com.mtx.trade.storage.local.config;

import org.apache.shardingsphere.infra.hint.HintManager;

/** 为一次 Storage 调用设置基于 SHA-256 的表路由提示。 */
public final class StorageShardingHint {

    private static final String STORAGE_TABLE = "trade_storage";
    private static final String STORAGE_BLOB_TABLE = "trade_storage_blob";

    private StorageShardingHint() {
    }

    public static HintManager open(byte[] sha256) {
        int shard = Sha256ShardingAlgorithm.shardIndex(sha256);
        HintManager hintManager = HintManager.getInstance();
        try {
            hintManager.addTableShardingValue(STORAGE_TABLE, shard);
            hintManager.addTableShardingValue(STORAGE_BLOB_TABLE, shard);
            return hintManager;
        } catch (RuntimeException e) {
            hintManager.close();
            throw e;
        }
    }
}
