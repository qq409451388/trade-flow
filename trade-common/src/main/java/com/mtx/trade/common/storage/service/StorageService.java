package com.mtx.trade.common.storage.service;


import com.mtx.trade.common.storage.entity.StorageDO;

/**
 * 原始数据存储业务接口。
 *
 * <p>负责将原始请求体写入 storage 与 storage_blob 分表，并提供按 id 查询元信息与原始字节的能力。</p>
 */
public interface StorageService {

    /**
     * 接收原始数据，计算 SHA-256，存入 storage 与 storage_blob，返回 StorageDO。
     *
     * @param sourceSystem 来源系统，参考 {@link com.mtx.trade.receiver.common.enums.SourceSystem}
     * @param contentType   内容类型，参考 {@link com.mtx.trade.receiver.common.enums.ContentType}
     * @param payload       原始请求体字节
     * @return 写入后的 StorageDO（id 已被 MyBatis-Plus 雪花ID 填充）
     */
    StorageDO saveRawData(int sourceSystem, int contentType, byte[] payload);

    /**
     * 根据 id 查询原始数据元信息。
     *
     * @param id 业务主键
     * @return StorageDO，不存在时返回 null
     */
    StorageDO getById(Long id);

    /**
     * 根据 id 查询原始请求体字节内容。
     *
     * @param id 业务主键
     * @return 原始字节内容，不存在时返回 null
     */
    byte[] getBlobContent(Long id);
}
