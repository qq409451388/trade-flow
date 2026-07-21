package com.mtx.trade.ingress.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.common.enums.EventAckStatus;
import com.mtx.trade.ingress.dto.EventIngestResult;
import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.service.OrderEventService;
import com.mtx.trade.ingress.service.db.OrderEventDbService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;

/**
 * 订单事件业务服务实现。
 */
@Service
@RequiredArgsConstructor
public class OrderEventServiceImpl implements OrderEventService {

    private final OrderEventDbService orderEventDbService;

    @Override
    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public EventIngestResult<OrderEventDO> createEvent(
            int sourceSystem, String thirdEventKey, long messageVersion, Long rawId, byte[] payloadSha256) {
        validate(thirdEventKey, messageVersion, rawId, payloadSha256);
        return saveAllVersions(sourceSystem, thirdEventKey, messageVersion, rawId, payloadSha256);
    }

    private EventIngestResult<OrderEventDO> saveAllVersions(
            int sourceSystem, String thirdEventKey, long messageVersion, Long rawId, byte[] payloadSha256) {
        OrderEventDO entity = newEvent(sourceSystem, thirdEventKey, messageVersion, rawId, payloadSha256);
        try {
            saveOrThrow(entity);
            return new EventIngestResult<>(entity, true);
        } catch (DuplicateKeyException e) {
            OrderEventDO existing = findExactVersion(sourceSystem, thirdEventKey, messageVersion);
            if (existing != null) {
                return new EventIngestResult<>(existing, false);
            }
            throw e;
        }
    }

    private OrderEventDO newEvent(
            int sourceSystem, String thirdEventKey, long messageVersion, Long rawId, byte[] payloadSha256) {
        OrderEventDO entity = new OrderEventDO();
        entity.setSourceSystem(sourceSystem);
        entity.setThirdEventKey(thirdEventKey);
        entity.setMessageVersion(messageVersion);
        entity.setRawId(rawId);
        entity.setPayloadSha256(payloadSha256);
        entity.setAcked(EventAckStatus.INIT.getCode());
        entity.setReceivedTime(LocalDateTime.now());
        return entity;
    }

    private void saveOrThrow(OrderEventDO entity) {
        if (!orderEventDbService.save(entity)) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR);
        }
    }

    private OrderEventDO findExactVersion(int sourceSystem, String thirdEventKey, long messageVersion) {
        return orderEventDbService.getOne(new LambdaQueryWrapper<OrderEventDO>()
                .eq(OrderEventDO::getSourceSystem, sourceSystem)
                .eq(OrderEventDO::getThirdEventKey, thirdEventKey)
                .eq(OrderEventDO::getMessageVersion, messageVersion), false);
    }

    private static void validate(String eventKey, long messageVersion, Long rawId, byte[] payloadSha256) {
        if (!StringUtils.hasText(eventKey) || eventKey.length() > 128 || messageVersion < 0 || rawId == null
                || payloadSha256 == null || payloadSha256.length != 32) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "event 消息版本或幂等参数无效");
        }
    }

    @Override
    public OrderEventDO getById(Long id) {
        return orderEventDbService.getById(id);
    }

}
