package com.mtx.trade.pipeline.service.impl;

import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.dto.OrderEventMessage;
import com.mtx.trade.pipeline.entity.OrderEventProcessLogDO;
import com.mtx.trade.pipeline.service.OrderEventProcessLogService;
import com.mtx.trade.pipeline.service.OrderPersistResult;
import com.mtx.trade.pipeline.service.db.OrderEventProcessLogDbService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

/** Pipeline 订单事件处理日志实现。 */
@Service
@RequiredArgsConstructor
public class OrderEventProcessLogServiceImpl implements OrderEventProcessLogService {

    private static final int MAX_REASON_LENGTH = 1024;

    private final OrderEventProcessLogDbService processLogDbService;

    @Override
    @Transactional(
            transactionManager = "pipelineTransactionManager",
            propagation = Propagation.REQUIRES_NEW,
            rollbackFor = Exception.class)
    public long recordSuccess(
            OrderEventMessage event,
            String streamRecordId,
            int triggerType,
            OrderPersistResult result,
            LocalDateTime startedTime,
            long startedNanos) {
        OrderEventProcessLogDO entity = base(event, streamRecordId, triggerType, startedTime, startedNanos);
        entity.setProcessStatus(result == OrderPersistResult.APPLIED ? STATUS_APPLIED : STATUS_IGNORED);
        return saveOrThrow(entity);
    }

    @Override
    @Transactional(
            transactionManager = "pipelineTransactionManager",
            propagation = Propagation.REQUIRES_NEW,
            rollbackFor = Exception.class)
    public long recordFailure(
            OrderEventMessage event,
            String streamRecordId,
            int triggerType,
            String failureStage,
            Throwable failure,
            LocalDateTime startedTime,
            long startedNanos) {
        Throwable actual = failure == null || failure.getCause() == null ? failure : failure.getCause();
        OrderEventProcessLogDO entity = base(event, streamRecordId, triggerType, startedTime, startedNanos);
        entity.setProcessStatus(STATUS_FAILED);
        entity.setFailureStage(failureStage);
        entity.setErrorCode(actual instanceof BusinessException businessException
                ? businessException.getCode() : ErrorCode.SYSTEM_ERROR.getCode());
        entity.setFailureReason(limitReason(actual));
        return saveOrThrow(entity);
    }

    @Override
    @Transactional(transactionManager = "pipelineTransactionManager",
            propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void recordIngressAck(long processLogId, boolean succeeded) {
        OrderEventProcessLogDO entity = new OrderEventProcessLogDO();
        entity.setId(processLogId);
        entity.setIngressAckStatus(succeeded ? DELIVERY_SUCCEEDED : DELIVERY_FAILED);
        entity.setIngressAckTime(succeeded ? LocalDateTime.now() : null);
        updateOrThrow(entity, "Ingress ACK");
    }

    @Override
    @Transactional(transactionManager = "pipelineTransactionManager",
            propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void recordRedisXack(long processLogId, boolean succeeded) {
        OrderEventProcessLogDO entity = new OrderEventProcessLogDO();
        entity.setId(processLogId);
        entity.setRedisXackStatus(succeeded ? DELIVERY_SUCCEEDED : DELIVERY_FAILED);
        updateOrThrow(entity, "Redis XACK");
    }

    private OrderEventProcessLogDO base(
            OrderEventMessage event,
            String streamRecordId,
            int triggerType,
            LocalDateTime startedTime,
            long startedNanos) {
        OrderEventProcessLogDO entity = new OrderEventProcessLogDO();
        if (event != null) {
            entity.setEventId(event.eventId());
            entity.setStorageId(event.storageId());
            entity.setPayloadSha256(event.storageSha256());
            entity.setEventKey(event.eventKey());
            entity.setMessageVersion(event.messageVersion());
        }
        entity.setStreamRecordId(streamRecordId);
        entity.setTriggerType(triggerType);
        entity.setIngressAckStatus(DELIVERY_PENDING);
        entity.setRedisXackStatus(triggerType == TRIGGER_ACTIVE_PULL
                ? DELIVERY_NOT_APPLICABLE : DELIVERY_PENDING);
        entity.setStartedTime(startedTime);
        entity.setFinishedTime(LocalDateTime.now());
        entity.setDurationMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos));
        return entity;
    }

    private long saveOrThrow(OrderEventProcessLogDO entity) {
        if (!processLogDbService.save(entity)) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "订单事件处理日志保存失败");
        }
        if (entity.getId() == null) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "订单事件处理日志ID未回填");
        }
        return entity.getId();
    }

    private void updateOrThrow(OrderEventProcessLogDO entity, String stage) {
        if (!processLogDbService.updateById(entity)) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "订单事件处理日志" + stage + "状态更新失败");
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
