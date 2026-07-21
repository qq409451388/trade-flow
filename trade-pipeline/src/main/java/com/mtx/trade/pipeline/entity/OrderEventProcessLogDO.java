package com.mtx.trade.pipeline.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** Pipeline 订单事件每次实际处理的审计流水。 */
@Data
@TableName("pipeline_order_event_log")
public class OrderEventProcessLogDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.AUTO)
    private Long id;
    private Long eventId;
    private String streamRecordId;
    private Integer triggerType;
    private Long storageId;
    private byte[] payloadSha256;
    private String eventKey;
    private Long messageVersion;
    private Integer processStatus;
    private String failureStage;
    private Integer errorCode;
    private String failureReason;
    private LocalDateTime startedTime;
    private LocalDateTime finishedTime;
    private Long durationMs;
    private LocalDateTime createTime;
}
