package com.mtx.trade.receiver.service.impl;

import com.mtx.trade.receiver.common.enums.EventStatus;
import com.mtx.trade.receiver.entity.OrderEventDO;
import com.mtx.trade.receiver.service.OrderEventService;
import com.mtx.trade.receiver.service.db.OrderEventDbService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * 订单事件业务服务实现。
 */
@Service
@RequiredArgsConstructor
public class OrderEventServiceImpl implements OrderEventService {

    private final OrderEventDbService orderEventDbService;

    @Override
    public OrderEventDO createEvent(int sourceSystem, String thirdEventKey, Long rawId, byte[] payloadSha256) {
        OrderEventDO entity = new OrderEventDO();
        entity.setSourceSystem(sourceSystem);
        entity.setThirdEventKey(thirdEventKey);
        entity.setRawId(rawId);
        entity.setPayloadSha256(payloadSha256);
        entity.setEventStatus(EventStatus.PENDING.getCode());
        entity.setReceivedTime(LocalDateTime.now());
        orderEventDbService.save(entity);
        return entity;
    }

    @Override
    public OrderEventDO getById(Long id) {
        return orderEventDbService.getById(id);
    }

    @Override
    public void markSuccess(Long eventId, Long executionId) {
        OrderEventDO entity = new OrderEventDO();
        entity.setId(eventId);
        entity.setEventStatus(EventStatus.SUCCESS.getCode());
        entity.setLastExecutionId(executionId);
        entity.setSuccessTime(LocalDateTime.now());
        orderEventDbService.updateById(entity);
    }

    @Override
    public void markFailed(Long eventId, Long executionId) {
        OrderEventDO entity = new OrderEventDO();
        entity.setId(eventId);
        entity.setEventStatus(EventStatus.FAILED.getCode());
        entity.setLastExecutionId(executionId);
        orderEventDbService.updateById(entity);
    }
}
