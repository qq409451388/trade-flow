package com.mtx.trade.storage.local;

import com.mtx.trade.storage.api.StorageMetadata;
import com.mtx.trade.storage.api.StorageReader;
import com.mtx.trade.storage.api.StorageRef;
import com.mtx.trade.storage.api.StorageWriteCommand;
import com.mtx.trade.storage.api.StorageWriter;
import com.mtx.trade.storage.local.entity.StorageBlobDO;
import com.mtx.trade.storage.local.entity.StorageDO;
import com.mtx.trade.storage.local.service.db.StorageBlobDbService;
import com.mtx.trade.storage.local.service.db.StorageDbService;
import lombok.RequiredArgsConstructor;
import org.springframework.transaction.annotation.Transactional;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;

/** 单机 MySQL Storage adapter。 */
@RequiredArgsConstructor
public class LocalStorageAdapter implements StorageWriter, StorageReader {

    private static final int BLOB_STORAGE_TYPE = 1;

    private final StorageDbService storageDbService;
    private final StorageBlobDbService storageBlobDbService;

    @Override
    @Transactional(transactionManager = "storageTransactionManager", rollbackFor = Exception.class)
    public StorageRef put(StorageWriteCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("storage 写入命令不能为空");
        }

        byte[] content = command.content();
        byte[] sha256 = sha256(content);
        StorageDO storage = new StorageDO();
        storage.setSourceSystem(command.sourceSystem());
        storage.setContentType(command.contentType());
        storage.setPayloadSha256(sha256);
        storage.setPayloadLength(content.length);
        storage.setContentStorageType(BLOB_STORAGE_TYPE);
        storage.setContentRef("");
        storage.setContentOffset(0L);
        storage.setContentLength(content.length);
        storage.setReceivedTime(command.receivedTime() == null ? LocalDateTime.now() : command.receivedTime());
        storageDbService.save(storage);

        StorageBlobDO blob = new StorageBlobDO();
        blob.setId(storage.getId());
        blob.setContent(content);
        storageBlobDbService.save(blob);
        return new StorageRef(storage.getId(), sha256, content.length);
    }

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

    private static byte[] sha256(byte[] content) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(content);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }
}
