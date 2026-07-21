package com.mtx.trade.ingress.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mtx.trade.ingress.entity.EventExecutionLogDO;
import com.mtx.trade.ingress.service.EventExecutionLogService;
import com.mtx.trade.ingress.service.db.EventExecutionLogDbService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 事件执行流水业务服务实现。
 */
@Service
@RequiredArgsConstructor
public class EventExecutionLogServiceImpl implements EventExecutionLogService {

    private final EventExecutionLogDbService eventExecutionLogDbService;

    @Override
    public EventExecutionLogDO log(int eventType, Long eventId, Long rawId, byte[] payloadSha256, int triggerType,
                                   int executionStatus, String message, String operatorName) {
        EventExecutionLogDO entity = new EventExecutionLogDO();
        entity.setEventType(eventType);
        entity.setEventId(eventId);
        entity.setRawId(rawId);
        entity.setPayloadSha256(payloadSha256);
        entity.setTriggerType(triggerType);
        entity.setExecutionStatus(executionStatus);
        entity.setMessage(message);
        entity.setOperatorName(operatorName);
        eventExecutionLogDbService.save(entity);
        return entity;
    }

    @Override
    public EventExecutionLogDO getById(Long id) {
        return eventExecutionLogDbService.getById(id);
    }

    @Override
    public List<EventExecutionLogDO> listByEvent(int eventType, Long eventId) {
        LambdaQueryWrapper<EventExecutionLogDO> wrapper = new LambdaQueryWrapper<>();
        wrapper.eq(EventExecutionLogDO::getEventType, eventType)
                .eq(EventExecutionLogDO::getEventId, eventId)
                .orderByDesc(EventExecutionLogDO::getCreateTime);
        return eventExecutionLogDbService.list(wrapper);
    }
}
