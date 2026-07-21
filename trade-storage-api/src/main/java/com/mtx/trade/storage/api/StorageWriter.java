package com.mtx.trade.storage.api;

/**
 * 原始数据写入端口。业务模块只依赖此接口，不感知数据库表和分片规则。
 */
public interface StorageWriter {

    /**
     * 如果相同 {@code (sourceSystem, sha256)} 的内容已存在，直接返回已有 StorageRef；
     * 否则写入新内容并返回。
     *
     * <p>用于第三方重推相同报文时避免存储层重复写入。SHA-256 负责分片路由，
     * 分片内 {@code (sourceSystem, sha256)} 唯一键负责并发竞争兜底。</p>
     *
     * @param command 写入命令
     * @return 已有或新建的 StorageRef
     * @throws StorageWriteException 新建时写入失败
     */
    StorageRef putIfAbsent(StorageWriteCommand command);
}
