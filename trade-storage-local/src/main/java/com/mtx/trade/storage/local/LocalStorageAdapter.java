package com.mtx.trade.storage.local;

import com.mtx.trade.storage.api.StorageIdGenerator;
import com.mtx.trade.storage.api.StorageRef;
import com.mtx.trade.storage.api.StorageWriteCommand;
import com.mtx.trade.storage.api.StorageWriteException;
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
public class LocalStorageAdapter implements StorageWriter {

    private static final int BLOB_STORAGE_TYPE = 1;

    private final StorageDbService storageDbService;
    private final StorageBlobDbService storageBlobDbService;
    private final StorageIdGenerator storageIdGenerator;

    @Override
    @Transactional(transactionManager = "storageTransactionManager", rollbackFor = Exception.class)
    public StorageRef put(StorageWriteCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("storage 写入命令不能为空");
        }

        byte[] content = command.content();
        byte[] sha256 = sha256(content);
        long storageId = storageIdGenerator.nextId();
        if (storageId <= 0) {
            throw new StorageWriteException("storage 域 ID 生成失败");
        }
        StorageDO storage = new StorageDO();
        storage.setId(storageId);
        storage.setSourceSystem(command.sourceSystem());
        storage.setContentType(command.contentType());
        storage.setPayloadSha256(sha256);
        storage.setPayloadLength(content.length);
        storage.setContentStorageType(BLOB_STORAGE_TYPE);
        storage.setContentRef("");
        storage.setContentOffset(0L);
        storage.setContentLength(content.length);
        storage.setReceivedTime(command.receivedTime() == null ? LocalDateTime.now() : command.receivedTime());
        if (!storageDbService.save(storage)) {
            throw new StorageWriteException("storage 元数据写入失败");
        }

        StorageBlobDO blob = new StorageBlobDO();
        blob.setId(storageId);
        blob.setContent(content);
        if (!storageBlobDbService.save(blob)) {
            throw new StorageWriteException("storage BLOB 写入失败");
        }
        return new StorageRef(storageId, sha256, content.length);
    }

    private static byte[] sha256(byte[] content) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(content);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }
}
