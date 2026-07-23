package com.mtx.trade.pipeline.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    @Override
    public long countActivePullFailures(long eventId) {
        return processLogDbService.count(new LambdaQueryWrapper<OrderEventProcessLogDO>()
                .eq(OrderEventProcessLogDO::getEventId, eventId)
                .eq(OrderEventProcessLogDO::getTriggerType, TRIGGER_ACTIVE_PULL)
                .eq(OrderEventProcessLogDO::getProcessStatus, STATUS_FAILED));
    }

    @Override
    public Set<Long> findAckOnlyEventIds(List<Long> eventIds, int terminalFailureAttempts) {
        if (eventIds == null || eventIds.isEmpty()) {
            return Set.of();
        }
        if (terminalFailureAttempts <= 0) {
            throw new IllegalArgumentException("terminalFailureAttempts必须为正数");
        }
        List<OrderEventProcessLogDO> logs = processLogDbService.list(
                new LambdaQueryWrapper<OrderEventProcessLogDO>()
                        .select(OrderEventProcessLogDO::getEventId,
                                OrderEventProcessLogDO::getProcessStatus,
                                OrderEventProcessLogDO::getTriggerType)
                        .in(OrderEventProcessLogDO::getEventId, eventIds)
                        .and(query -> query
                                .in(OrderEventProcessLogDO::getProcessStatus, STATUS_APPLIED, STATUS_IGNORED)
                                .or(failure -> failure
                                        .eq(OrderEventProcessLogDO::getTriggerType, TRIGGER_ACTIVE_PULL)
                                        .eq(OrderEventProcessLogDO::getProcessStatus, STATUS_FAILED))));
        Set<Long> ackOnly = new HashSet<>();
        Map<Long, Integer> activePullFailures = new HashMap<>();
        for (OrderEventProcessLogDO log : logs) {
            if (log.getProcessStatus() == STATUS_APPLIED || log.getProcessStatus() == STATUS_IGNORED) {
                ackOnly.add(log.getEventId());
            } else if (log.getTriggerType() == TRIGGER_ACTIVE_PULL
                    && log.getProcessStatus() == STATUS_FAILED) {
                activePullFailures.merge(log.getEventId(), 1, Integer::sum);
            }
        }
        activePullFailures.forEach((eventId, attempts) -> {
            if (attempts >= terminalFailureAttempts) ackOnly.add(eventId);
        });
        return ackOnly;
    }

    @Override
    @Transactional(transactionManager = "pipelineTransactionManager",
            propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void recordIngressAckByEventIds(List<Long> eventIds, boolean succeeded) {
        if (eventIds == null || eventIds.isEmpty()) return;
        processLogDbService.update(new LambdaUpdateWrapper<OrderEventProcessLogDO>()
                .in(OrderEventProcessLogDO::getEventId, eventIds)
                .ne(OrderEventProcessLogDO::getIngressAckStatus, DELIVERY_SUCCEEDED)
                .set(OrderEventProcessLogDO::getIngressAckStatus,
                        succeeded ? DELIVERY_SUCCEEDED : DELIVERY_FAILED)
                .set(OrderEventProcessLogDO::getIngressAckTime, succeeded ? LocalDateTime.now() : null));
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
