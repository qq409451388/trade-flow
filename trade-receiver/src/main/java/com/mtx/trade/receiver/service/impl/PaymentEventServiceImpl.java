package com.mtx.trade.receiver.service.impl;

import com.mtx.trade.receiver.common.enums.EventStatus;
import com.mtx.trade.receiver.entity.PaymentEventDO;
import com.mtx.trade.receiver.service.PaymentEventService;
import com.mtx.trade.receiver.service.db.PaymentEventDbService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * 支付事件业务服务实现。
 */
@Service
@RequiredArgsConstructor
public class PaymentEventServiceImpl implements PaymentEventService {

    private final PaymentEventDbService paymentEventDbService;

    @Override
    public PaymentEventDO createEvent(int sourceSystem, String eventKey, Long rawId, byte[] payloadSha256) {
        PaymentEventDO entity = new PaymentEventDO();
        entity.setSourceSystem(sourceSystem);
        entity.setEventKey(eventKey);
        entity.setRawId(rawId);
        entity.setPayloadSha256(payloadSha256);
        entity.setEventStatus(EventStatus.PENDING.getCode());
        entity.setReceivedTime(LocalDateTime.now());
        paymentEventDbService.save(entity);
        return entity;
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
