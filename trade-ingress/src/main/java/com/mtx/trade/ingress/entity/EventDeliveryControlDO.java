package com.mtx.trade.ingress.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** 每个事件通道的持久化投递熔断控制状态。 */
@Data
@TableName("trade_event_delivery_control")
public class EventDeliveryControlDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.INPUT)
    private Integer contentType;
    private Integer circuitStatus;
    private LocalDateTime failureWindowStart;
    private Integer failureCount;
    private LocalDateTime openedTime;
    private LocalDateTime nextHealthCheckTime;
    private Integer healthSuccessCount;
    private LocalDateTime lastFailureTime;
    private String lastFailureReason;
    private String recoveryOwner;
    private LocalDateTime recoveryLeaseUntil;
    private Integer version;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
