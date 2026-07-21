package com.mtx.trade.storage.local;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mtx.trade.storage.api.StorageIdGenerator;
import com.mtx.trade.storage.api.StorageRef;
import com.mtx.trade.storage.api.StorageWriteCommand;
import com.mtx.trade.storage.api.StorageWriteException;
import com.mtx.trade.storage.api.StorageWriter;
import com.mtx.trade.storage.local.config.StorageShardingHint;
import com.mtx.trade.storage.local.entity.StorageBlobDO;
import com.mtx.trade.storage.local.entity.StorageDO;
import com.mtx.trade.storage.local.service.db.StorageBlobDbService;
import com.mtx.trade.storage.local.service.db.StorageDbService;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.hint.HintManager;
import org.springframework.dao.DuplicateKeyException;
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
    public StorageRef putIfAbsent(StorageWriteCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("storage 写入命令不能为空");
        }
        byte[] sha256 = sha256(command.content());
        try (HintManager ignored = StorageShardingHint.open(sha256)) {
            StorageDO existing = findExisting(command.sourceSystem(), sha256, false);
            if (existing != null) {
                return toRef(existing);
            }
            return doInsert(command, sha256);
        }
    }

    private StorageRef doInsert(StorageWriteCommand command, byte[] sha256) {
        byte[] content = command.content();
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
        try {
            if (!storageDbService.save(storage)) {
                throw new StorageWriteException("storage 元数据写入失败");
            }
        } catch (DuplicateKeyException e) {
            StorageDO existing = findExisting(command.sourceSystem(), sha256, true);
            if (existing != null) {
                return toRef(existing);
            }
            throw new StorageWriteException("storage 元数据主键或内容幂等键冲突", e);
        }

        StorageBlobDO blob = new StorageBlobDO();
        blob.setId(storageId);
        blob.setPayloadSha256(sha256);
        blob.setContent(content);
        if (!storageBlobDbService.save(blob)) {
            throw new StorageWriteException("storage BLOB 写入失败");
        }
        return new StorageRef(storageId, sha256, content.length);
    }

    private StorageDO findExisting(int sourceSystem, byte[] sha256, boolean forUpdate) {
        LambdaQueryWrapper<StorageDO> query = new LambdaQueryWrapper<StorageDO>()
                .eq(StorageDO::getPayloadSha256, sha256)
                .eq(StorageDO::getSourceSystem, sourceSystem)
                .last(forUpdate ? "LIMIT 1 FOR UPDATE" : "LIMIT 1");
        return storageDbService.getOne(query, false);
    }

    private static StorageRef toRef(StorageDO storage) {
        return new StorageRef(storage.getId(), storage.getPayloadSha256(), storage.getContentLength());
    }

    private static byte[] sha256(byte[] content) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(content);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }
}
