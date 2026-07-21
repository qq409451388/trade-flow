package com.mtx.trade.ingress.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.common.enums.EventAckStatus;
import com.mtx.trade.ingress.dto.EventIngestResult;
import com.mtx.trade.ingress.entity.PaymentEventDO;
import com.mtx.trade.ingress.service.PaymentEventService;
import com.mtx.trade.ingress.service.db.PaymentEventDbService;
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
    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public EventIngestResult<PaymentEventDO> createEvent(
            int sourceSystem, String thirdEventKey, long messageVersion, Long rawId, byte[] payloadSha256) {
        validate(thirdEventKey, messageVersion, rawId, payloadSha256);
        return saveAllVersions(sourceSystem, thirdEventKey, messageVersion, rawId, payloadSha256);
    }

    private EventIngestResult<PaymentEventDO> saveAllVersions(
            int sourceSystem, String thirdEventKey, long messageVersion, Long rawId, byte[] payloadSha256) {
        PaymentEventDO entity = newEvent(sourceSystem, thirdEventKey, messageVersion, rawId, payloadSha256);
        try {
            saveOrThrow(entity);
            return new EventIngestResult<>(entity, true);
        } catch (DuplicateKeyException e) {
            PaymentEventDO existing = findExactVersion(sourceSystem, thirdEventKey, messageVersion);
            if (existing != null) {
                return new EventIngestResult<>(existing, false);
            }
            throw e;
        }
    }

    private PaymentEventDO newEvent(
            int sourceSystem, String thirdEventKey, long messageVersion, Long rawId, byte[] payloadSha256) {
        PaymentEventDO entity = new PaymentEventDO();
        entity.setSourceSystem(sourceSystem);
        entity.setThirdEventKey(thirdEventKey);
        entity.setMessageVersion(messageVersion);
        entity.setRawId(rawId);
        entity.setPayloadSha256(payloadSha256);
        entity.setAcked(EventAckStatus.INIT.getCode());
        entity.setReceivedTime(LocalDateTime.now());
        return entity;
    }

    private void saveOrThrow(PaymentEventDO entity) {
        if (!paymentEventDbService.save(entity)) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR);
        }
    }

    private PaymentEventDO findExactVersion(int sourceSystem, String thirdEventKey, long messageVersion) {
        return paymentEventDbService.getOne(new LambdaQueryWrapper<PaymentEventDO>()
                .eq(PaymentEventDO::getSourceSystem, sourceSystem)
                .eq(PaymentEventDO::getThirdEventKey, thirdEventKey)
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

}
