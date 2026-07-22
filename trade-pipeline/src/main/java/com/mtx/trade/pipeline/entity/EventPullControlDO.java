package com.mtx.trade.pipeline.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** Pipeline 按事件通道维护的主动拉取租约。 */
@Data
@TableName("pipeline_event_pull_control")
public class EventPullControlDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.INPUT)
    private Integer contentType;
    private String leaseOwner;
    private LocalDateTime leaseUntil;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
