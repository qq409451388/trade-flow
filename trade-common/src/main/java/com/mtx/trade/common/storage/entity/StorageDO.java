package com.mtx.trade.common.storage.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 对应表 trade_storage（分表：trade_storage_00 ~ trade_storage_99）。
 *
 * <p>记录原始请求体的元信息，以及内容存储位置。payload 字节本身存放在
 * {@link StorageBlobDO}（BLOB）或外部归档文件中。</p>
 */
@Data
@TableName("trade_storage")
public class StorageDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 雪花ID，同时作为分片键，分片规则 id % 100
     */
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /**
     * 来源系统：0未知；1富友
     */
    private Integer sourceSystem;

    /**
     * 内容类型：1订单；2支付
     */
    private Integer contentType;

    /**
     * 原始请求体字节SHA-256
     */
    private byte[] payloadSha256;

    /**
     * 原始请求体字节长度
     */
    private Integer payloadLength;

    /**
     * 存储类型：1 BLOB；2本地归档；3 OSS归档
     */
    private Integer contentStorageType;

    /**
     * 归档文件相对路径或对象Key；BLOB类型为空
     */
    private String contentRef;

    /**
     * 内容在归档文件内的字节偏移量；BLOB类型为0
     */
    private Long contentOffset;

    /**
     * 归档文件内存储内容长度；未归档时为0
     */
    private Integer contentLength;

    /**
     * 原始数据接收时间
     */
    private LocalDateTime receivedTime;

    /**
     * 记录创建时间
     */
    private LocalDateTime createTime;

    /**
     * 记录更新时间
     */
    private LocalDateTime updateTime;
}
