package com.mtx.trade.storage.local.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** Storage 元数据持久化实体。 */
@Data
@TableName("trade_storage")
public class StorageDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.INPUT)
    private Long id;
    private Integer sourceSystem;
    private Integer contentType;
    private byte[] payloadSha256;
    private Integer payloadLength;
    private Integer contentStorageType;
    private String contentRef;
    private Long contentOffset;
    private Integer contentLength;
    private LocalDateTime receivedTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
