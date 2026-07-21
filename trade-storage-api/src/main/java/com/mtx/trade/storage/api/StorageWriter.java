package com.mtx.trade.storage.api;

/**
 * 原始数据写入端口。业务模块只依赖此接口，不感知数据库表和分片规则。
 */
public interface StorageWriter {

    StorageRef put(StorageWriteCommand command);
}
