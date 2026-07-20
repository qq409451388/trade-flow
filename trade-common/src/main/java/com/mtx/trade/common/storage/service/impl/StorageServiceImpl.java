package com.mtx.trade.common.storage.service.impl;

import com.mtx.trade.common.enums.ContentStorageType;
import com.mtx.trade.common.storage.entity.StorageBlobDO;
import com.mtx.trade.common.storage.entity.StorageDO;
import com.mtx.trade.common.storage.service.StorageService;
import com.mtx.trade.common.storage.service.db.StorageBlobDbService;
import com.mtx.trade.common.storage.service.db.StorageDbService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;

/**
 * 原始数据存储业务实现。
 *
 * <p>当前实现采用 BLOB 存储模式：原始字节直接写入 trade_storage_blob 分表，
 * trade_storage 仅记录元信息与 SHA-256 摘要。后续可扩展为本地归档 / OSS 归档模式。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StorageServiceImpl implements StorageService {

    private final StorageDbService storageDbService;
    private final StorageBlobDbService storageBlobDbService;

    @Override
    public StorageDO saveRawData(int sourceSystem, int contentType, byte[] payload) {
        byte[] sha256 = sha256(payload);

        StorageDO storage = new StorageDO();
        storage.setSourceSystem(sourceSystem);
        storage.setContentType(contentType);
        storage.setPayloadSha256(sha256);
        storage.setPayloadLength(payload == null ? 0 : payload.length);
        storage.setContentStorageType(ContentStorageType.BLOB.getCode());
        storage.setContentRef("");
        storage.setContentOffset(0L);
        storage.setContentLength(0);
        storage.setReceivedTime(LocalDateTime.now());

        // save 后 storage.id 已被 MyBatis-Plus（雪花ID）填充
        storageDbService.save(storage);

        StorageBlobDO blob = new StorageBlobDO();
        blob.setId(storage.getId());
        blob.setContent(payload);
        storageBlobDbService.save(blob);

        return storage;
    }

    @Override
    public StorageDO getById(Long id) {
        return storageDbService.getById(id);
    }

    @Override
    public byte[] getBlobContent(Long id) {
        StorageBlobDO blob = storageBlobDbService.getById(id);
        return blob == null ? null : blob.getContent();
    }

    /**
     * 计算字节内容的 SHA-256 摘要。
     *
     * @param data 原始字节
     * @return 32 字节摘要；入参为空时返回空字节数组
     */
    private static byte[] sha256(byte[] data) {
        if (data == null || data.length == 0) {
            return new byte[0];
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(data);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 为 JDK 标准算法，理论上不会缺失
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }
}
