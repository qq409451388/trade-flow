package com.mtx.trade.storage.local;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mtx.trade.storage.api.StorageKey;
import com.mtx.trade.storage.api.StorageMetadata;
import com.mtx.trade.storage.api.StorageReader;
import com.mtx.trade.storage.local.config.StorageShardingHint;
import com.mtx.trade.storage.local.entity.StorageBlobDO;
import com.mtx.trade.storage.local.entity.StorageDO;
import com.mtx.trade.storage.local.service.db.StorageBlobDbService;
import com.mtx.trade.storage.local.service.db.StorageDbService;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.hint.HintManager;
import org.springframework.transaction.annotation.Transactional;

/** 单机 MySQL Storage 只读 adapter。 */
@RequiredArgsConstructor
public class LocalStorageReaderAdapter implements StorageReader {

    private final StorageDbService storageDbService;
    private final StorageBlobDbService storageBlobDbService;

    @Override
    @Transactional(transactionManager = "storageTransactionManager", readOnly = true)
    public StorageMetadata getMetadata(StorageKey key) {
        if (key == null) {
            return null;
        }
        byte[] sha256 = key.sha256();
        try (HintManager ignored = StorageShardingHint.open(sha256)) {
            StorageDO storage = storageDbService.getOne(new LambdaQueryWrapper<StorageDO>()
                    .eq(StorageDO::getPayloadSha256, sha256)
                    .eq(StorageDO::getId, key.storageId()), false);
            if (storage == null) {
                return null;
            }
            return new StorageMetadata(
                    storage.getId(),
                    storage.getSourceSystem(),
                    storage.getContentType(),
                    storage.getPayloadSha256(),
                    storage.getPayloadLength(),
                    storage.getReceivedTime());
        }
    }

    @Override
    @Transactional(transactionManager = "storageTransactionManager", readOnly = true)
    public byte[] getContent(StorageKey key) {
        if (key == null) {
            return null;
        }
        byte[] sha256 = key.sha256();
        try (HintManager ignored = StorageShardingHint.open(sha256)) {
            StorageBlobDO blob = storageBlobDbService.getOne(new LambdaQueryWrapper<StorageBlobDO>()
                    .eq(StorageBlobDO::getPayloadSha256, sha256)
                    .eq(StorageBlobDO::getId, key.storageId()), false);
            return blob == null || blob.getContent() == null ? null : blob.getContent().clone();
        }
    }
}
