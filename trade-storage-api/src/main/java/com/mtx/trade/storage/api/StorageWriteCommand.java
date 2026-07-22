package com.mtx.trade.storage.api;

import java.time.LocalDateTime;

/**
 * Storage 写入命令。
 *
 * @param sourceSystem 来源系统编码
 * @param contentType 内容类型编码
 * @param content 原始内容字节
 * @param receivedTime 原始内容接收时间；为空时由 adapter 使用当前时间
 */
public record StorageWriteCommand(
        int sourceSystem,
        int contentType,
        byte[] content,
        LocalDateTime receivedTime) {

    public StorageWriteCommand {
        if (content == null) {
            throw new IllegalArgumentException("content must not be null");
        }
        content = content.clone();
    }

    @Override
    public byte[] content() {
        return content.clone();
    }
}
