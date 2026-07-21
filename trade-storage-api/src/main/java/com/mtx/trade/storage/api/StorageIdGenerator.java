package com.mtx.trade.storage.api;

/** Storage 主键生成端口，由接入应用提供具体的领域 ID 实现。 */
@FunctionalInterface
public interface StorageIdGenerator {

    long nextId();
}
