package com.mtx.trade.common.storage.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 对应表 trade_storage_blob（分表：trade_storage_blob_00 ~ trade_storage_blob_99）。
 *
 * <p>存储原始请求体字节，id 与 {@link StorageDO#getId()} 一致，并使用相同分片规则，
 * 以保证同一笔数据落在相同分表下，便于联合查询。</p>
 */
@Data
@TableName("trade_storage_blob")
public class StorageBlobDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 与 trade_storage.id 一致，并使用相同分片
     */
    @TableId(type = IdType.INPUT)
    private Long id;

    /**
     * 原始请求体字节
     */
    private byte[] content;

    /**
     * 记录创建时间
     */
    private LocalDateTime createTime;
}
