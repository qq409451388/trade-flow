package com.mtx.trade.ingress.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** Storage 已落库但 event 未正常落库的失败审计。 */
@Data
@TableName("trade_event_ingest_failure_log")
public class EventIngestFailureLogDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.AUTO)
    private Long id;
    private Integer sourceSystem;
    private Integer contentType;
    private Long rawId;
    private byte[] payloadSha256;
    private String failureStage;
    private Integer errorCode;
    private String failureReason;
    private String thirdEventKey;
    private Long messageVersion;
    private LocalDateTime createTime;
}
