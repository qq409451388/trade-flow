package com.mtx.trade.ingress.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 事件执行流水（trade_event_execution_log）。
 */
@Data
@TableName("trade_event_execution_log")
public class EventExecutionLogDO implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 事件自增主键
     */
    @TableId(type = IdType.AUTO)
    private Long id;

    /**
     * 事件类型：1订单；2支付
     */
    private Integer eventType;

    /**
     * 关联订单事件或支付事件ID
     */
    private Long eventId;

    /**
     * 关联trade_storage.id，事件删除后仍可定位原始报文
     */
    private Long rawId;

    /**
     * 原始请求体字节SHA-256，与 rawId 共同定位 Storage。
     */
    private byte[] payloadSha256;

    /**
     * 触发方式：1首次消费；2自动重试；3人工重跑；4批量回放
     */
    private Integer triggerType;

    /**
     * 执行结果：1成功；2失败；3忽略
     */
    private Integer executionStatus;

    /**
     * 执行结果说明，失败时记录错误摘要，忽略时记录忽略原因
     */
    private String message;

    /**
     * 操作人，自动执行时为空
     */
    private String operatorName;

    /**
     * 执行完成及流水写入时间
     */
    private LocalDateTime createTime;
}
