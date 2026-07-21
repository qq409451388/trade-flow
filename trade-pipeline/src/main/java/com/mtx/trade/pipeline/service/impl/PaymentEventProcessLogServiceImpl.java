package com.mtx.trade.pipeline.service.impl;

import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.dto.PaymentEventMessage;
import com.mtx.trade.pipeline.entity.PaymentEventProcessLogDO;
import com.mtx.trade.pipeline.service.PaymentEventProcessLogService;
import com.mtx.trade.pipeline.service.PaymentPersistResult;
import com.mtx.trade.pipeline.service.db.PaymentEventProcessLogDbService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

/** 支付事件处理日志独立事务实现。 */
@Service
@RequiredArgsConstructor
public class PaymentEventProcessLogServiceImpl implements PaymentEventProcessLogService {

    private static final int MAX_REASON_LENGTH = 1024;

    private final PaymentEventProcessLogDbService processLogDbService;

    @Override
    @Transactional(transactionManager = "pipelineTransactionManager",
            propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void recordSuccess(
            PaymentEventMessage event,
            String streamRecordId,
            int triggerType,
            PaymentPersistResult result,
            LocalDateTime startedTime,
            long startedNanos) {
        PaymentEventProcessLogDO entity = base(event, streamRecordId, triggerType, startedTime, startedNanos);
        entity.setProcessStatus(result == PaymentPersistResult.APPLIED ? STATUS_APPLIED : STATUS_IGNORED);
        saveOrThrow(entity);
    }

    @Override
    @Transactional(transactionManager = "pipelineTransactionManager",
            propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void recordFailure(
            PaymentEventMessage event,
            String streamRecordId,
            int triggerType,
            String failureStage,
            Throwable failure,
            LocalDateTime startedTime,
            long startedNanos) {
        Throwable actual = failure == null || failure.getCause() == null ? failure : failure.getCause();
        PaymentEventProcessLogDO entity = base(event, streamRecordId, triggerType, startedTime, startedNanos);
        entity.setProcessStatus(STATUS_FAILED);
        entity.setFailureStage(failureStage);
        entity.setErrorCode(actual instanceof BusinessException businessException
                ? businessException.getCode() : ErrorCode.SYSTEM_ERROR.getCode());
        entity.setFailureReason(limitReason(actual));
        saveOrThrow(entity);
    }

    private PaymentEventProcessLogDO base(
            PaymentEventMessage event,
            String streamRecordId,
            int triggerType,
            LocalDateTime startedTime,
            long startedNanos) {
        PaymentEventProcessLogDO entity = new PaymentEventProcessLogDO();
        if (event != null) {
            entity.setEventId(event.eventId());
            entity.setStorageId(event.storageId());
            entity.setPayloadSha256(event.storageSha256());
            entity.setEventKey(event.eventKey());
            entity.setMessageVersion(event.messageVersion());
        }
        entity.setStreamRecordId(streamRecordId);
        entity.setTriggerType(triggerType);
        entity.setStartedTime(startedTime);
        entity.setFinishedTime(LocalDateTime.now());
        entity.setDurationMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos));
        return entity;
    }

    private void saveOrThrow(PaymentEventProcessLogDO entity) {
        if (!processLogDbService.save(entity)) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "支付事件处理日志保存失败");
        }
    }

    private static String limitReason(Throwable failure) {
        String reason = failure == null ? null : failure.getMessage();
        if (!StringUtils.hasText(reason)) {
            reason = failure == null ? "unknown" : failure.getClass().getName();
        }
        return reason.length() <= MAX_REASON_LENGTH ? reason : reason.substring(0, MAX_REASON_LENGTH);
    }
}
