package com.mtx.trade.receiver.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.receiver.common.enums.EventStatus;
import com.mtx.trade.receiver.dto.EventIngestResult;
import com.mtx.trade.receiver.entity.PaymentEventDO;
import com.mtx.trade.receiver.service.PaymentEventService;
import com.mtx.trade.receiver.service.db.PaymentEventDbService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;

/**
 * 支付事件业务服务实现。
 */
@Service
@RequiredArgsConstructor
public class PaymentEventServiceImpl implements PaymentEventService {

    private final PaymentEventDbService paymentEventDbService;

    @Override
    @Transactional(transactionManager = "transactionManager", rollbackFor = Exception.class)
    public EventIngestResult<PaymentEventDO> createEvent(
            int sourceSystem, String eventKey, long messageVersion, Long rawId, byte[] payloadSha256) {
        validate(eventKey, messageVersion, rawId, payloadSha256);
        return saveAllVersions(sourceSystem, eventKey, messageVersion, rawId, payloadSha256);
    }

    private EventIngestResult<PaymentEventDO> saveAllVersions(
            int sourceSystem, String eventKey, long messageVersion, Long rawId, byte[] payloadSha256) {
        PaymentEventDO entity = newEvent(sourceSystem, eventKey, messageVersion, rawId, payloadSha256);
        try {
            saveOrThrow(entity);
            return new EventIngestResult<>(entity, true);
        } catch (DuplicateKeyException e) {
            PaymentEventDO existing = findExactVersion(sourceSystem, eventKey, messageVersion);
            if (existing != null) {
                return new EventIngestResult<>(existing, false);
            }
            throw e;
        }
    }

    private PaymentEventDO newEvent(
            int sourceSystem, String eventKey, long messageVersion, Long rawId, byte[] payloadSha256) {
        PaymentEventDO entity = new PaymentEventDO();
        entity.setSourceSystem(sourceSystem);
        entity.setEventKey(eventKey);
        entity.setMessageVersion(messageVersion);
        entity.setRawId(rawId);
        entity.setPayloadSha256(payloadSha256);
        entity.setEventStatus(EventStatus.PENDING.getCode());
        entity.setReceivedTime(LocalDateTime.now());
        return entity;
    }

    private void saveOrThrow(PaymentEventDO entity) {
        if (!paymentEventDbService.save(entity)) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR);
        }
    }

    private PaymentEventDO findExactVersion(int sourceSystem, String eventKey, long messageVersion) {
        return paymentEventDbService.getOne(new LambdaQueryWrapper<PaymentEventDO>()
                .eq(PaymentEventDO::getSourceSystem, sourceSystem)
                .eq(PaymentEventDO::getEventKey, eventKey)
                .eq(PaymentEventDO::getMessageVersion, messageVersion), false);
    }

    private static void validate(String eventKey, long messageVersion, Long rawId, byte[] payloadSha256) {
        if (!StringUtils.hasText(eventKey) || eventKey.length() > 128 || messageVersion < 0 || rawId == null
                || payloadSha256 == null || payloadSha256.length != 32) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "event 消息版本或幂等参数无效");
        }
    }

    @Override
    public PaymentEventDO getById(Long id) {
        return paymentEventDbService.getById(id);
    }

    @Override
    public void markSuccess(Long eventId, Long executionId) {
        PaymentEventDO entity = new PaymentEventDO();
        entity.setId(eventId);
        entity.setEventStatus(EventStatus.SUCCESS.getCode());
        entity.setLastExecutionId(executionId);
        entity.setSuccessTime(LocalDateTime.now());
        paymentEventDbService.updateById(entity);
    }

    @Override
    public void markFailed(Long eventId, Long executionId) {
        PaymentEventDO entity = new PaymentEventDO();
        entity.setId(eventId);
        entity.setEventStatus(EventStatus.FAILED.getCode());
        entity.setLastExecutionId(executionId);
        paymentEventDbService.updateById(entity);
    }
}
