package com.mtx.trade.storage.api;

/**
 * 原始数据读取端口。当前由本地 MySQL adapter 实现，后续可替换为远程客户端。
 */
public interface StorageReader {

    StorageMetadata getMetadata(StorageKey key);

    byte[] getContent(StorageKey key);
}
