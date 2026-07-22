package com.mtx.trade.pipeline.service;

import com.mtx.trade.pipeline.dto.PaymentEventMessage;

import java.time.LocalDateTime;

/** 支付事件每次实际处理的独立审计。 */
public interface PaymentEventProcessLogService {

    int TRIGGER_STREAM = 1;
    int TRIGGER_PEL_RECLAIM = 2;
    int TRIGGER_ACTIVE_PULL = 3;

    int STATUS_APPLIED = 1;
    int STATUS_IGNORED = 2;
    int STATUS_FAILED = 3;
    int DELIVERY_PENDING = 0;
    int DELIVERY_SUCCEEDED = 1;
    int DELIVERY_FAILED = 2;
    int DELIVERY_NOT_APPLICABLE = 3;

    long recordSuccess(
            PaymentEventMessage event,
            String streamRecordId,
            int triggerType,
            PaymentPersistResult result,
            LocalDateTime startedTime,
            long startedNanos);

    long recordFailure(
            PaymentEventMessage event,
            String streamRecordId,
            int triggerType,
            String failureStage,
            Throwable failure,
            LocalDateTime startedTime,
            long startedNanos);

    void recordIngressAck(long processLogId, boolean succeeded);

    void recordRedisXack(long processLogId, boolean succeeded);
}
