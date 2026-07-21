package com.mtx.trade.ingress.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 支付事件（trade_payment_event）。
 */
@Data
@TableName("trade_payment_event")
public class PaymentEventDO implements Serializable {

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
     * 事件唯一键，用于幂等
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
     * Pipeline 接管状态：0未ACK；1已ACK
     */
    private Integer acked;

    /**
     * Pipeline ACK 时间
     */
    private LocalDateTime ackedTime;

    /**
     * 15分钟后定时补发成功次数
     */
    private Integer autoRedeliveryCount;

    /**
     * 最近一次定时补发成功时间
     */
    private LocalDateTime lastRedeliveryTime;

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
