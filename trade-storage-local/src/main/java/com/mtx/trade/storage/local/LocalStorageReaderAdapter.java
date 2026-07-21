package com.mtx.trade.storage.local;

import com.mtx.trade.storage.api.StorageMetadata;
import com.mtx.trade.storage.api.StorageReader;
import com.mtx.trade.storage.local.entity.StorageBlobDO;
import com.mtx.trade.storage.local.entity.StorageDO;
import com.mtx.trade.storage.local.service.db.StorageBlobDbService;
import com.mtx.trade.storage.local.service.db.StorageDbService;
import lombok.RequiredArgsConstructor;
import org.springframework.transaction.annotation.Transactional;

/** 单机 MySQL Storage 只读 adapter。 */
@RequiredArgsConstructor
public class LocalStorageReaderAdapter implements StorageReader {

    private final StorageDbService storageDbService;
    private final StorageBlobDbService storageBlobDbService;

    @Override
    @Transactional(transactionManager = "storageTransactionManager", readOnly = true)
    public StorageMetadata getMetadata(Long storageId) {
        if (storageId == null) {
            return null;
        }
        StorageDO storage = storageDbService.getById(storageId);
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

    @Override
    @Transactional(transactionManager = "storageTransactionManager", readOnly = true)
    public byte[] getContent(Long storageId) {
        if (storageId == null) {
            return null;
        }
        StorageBlobDO blob = storageBlobDbService.getById(storageId);
        return blob == null || blob.getContent() == null ? null : blob.getContent().clone();
    }
}
