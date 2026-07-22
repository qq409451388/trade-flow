package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.constants.RedisKeyConstants;
import com.mtx.trade.ingress.config.StorageWriteProperties;
import com.mtx.trade.ingress.utils.RedisLock;
import com.mtx.trade.storage.api.StorageRef;
import com.mtx.trade.storage.api.StorageWriteCommand;
import com.mtx.trade.storage.api.StorageWriteException;
import com.mtx.trade.storage.api.StorageWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Ingress 侧 Storage 写入协调服务。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StorageWriteService {

    private final StorageWriter storageWriter;
    private final RedisLock redisLock;
    private final StorageWriteProperties properties;

    /**
     * 使用内容幂等键串行化并发写入，数据库唯一键仍作为最终一致性保障。
     */
    public StorageRef putIfAbsent(StorageWriteCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("storage 写入命令不能为空");
        }
        String lockKey = buildLockKey(command);
        try {
            redisLock.acquireLock(lockKey, properties.getLockWait(), properties.getLockLease());
        } catch (RuntimeException e) {
            throw new StorageWriteException("storage Redis lock acquire failed", e);
        }
        try {
            return storageWriter.putIfAbsent(command);
        } finally {
            if (!redisLock.releaseLock(lockKey)) {
                log.warn("[Storage Lock] 🔄 Redis lock release failed; lease expiry will release it. key={}", lockKey);
            }
        }
    }

    private static String buildLockKey(StorageWriteCommand command) {
        byte[] sha256 = sha256(command.content());
        return RedisKeyConstants.STORAGE_PUT_IF_ABSENT_LOCK
                + command.sourceSystem() + ":" + HexFormat.of().formatHex(sha256);
    }

    private static byte[] sha256(byte[] content) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(content);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }
}
