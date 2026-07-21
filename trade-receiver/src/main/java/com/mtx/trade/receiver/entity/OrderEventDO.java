package com.mtx.trade.receiver.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 订单事件（trade_order_event）。
 */
@Data
@TableName("trade_order_event")
public class OrderEventDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 事件自增主键
     */
    @TableId(type = IdType.AUTO)
    private Long id;

    /**
     * 来源系统：0未知；1富友
     */
    private Integer sourceSystem;

    /**
     * 第三方事件唯一键，用于幂等
     */
    private String thirdEventKey;

    /**
     * 第三方消息版本，来源适配器统一转换为非负整数
     */
    private Long messageVersion;

    /**
     * 关联trade_storage.id
     */
    private Long rawId;

    /**
     * 原始请求体字节SHA-256
     */
    private byte[] payloadSha256;

    /**
     * 事件状态：0待执行；1成功；2失败；3忽略
     */
    private Integer eventStatus;

    /**
     * 最近一次执行流水ID
     */
    private Long lastExecutionId;

    /**
     * 原始数据接收时间
     */
    private LocalDateTime receivedTime;

    /**
     * 执行成功时间
     */
    private LocalDateTime successTime;

    /**
     * 记录创建时间
     */
    private LocalDateTime createTime;

    /**
     * 记录更新时间
     */
    private LocalDateTime updateTime;
}
