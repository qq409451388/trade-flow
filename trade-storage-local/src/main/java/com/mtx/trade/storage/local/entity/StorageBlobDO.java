package com.mtx.trade.storage.local.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** Storage BLOB 持久化实体，与元数据使用相同 id 和分片编号。 */
@Data
@TableName("trade_storage_blob")
public class StorageBlobDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.INPUT)
    private Long id;
    private byte[] payloadSha256;
    private byte[] content;
    private LocalDateTime createTime;
}
