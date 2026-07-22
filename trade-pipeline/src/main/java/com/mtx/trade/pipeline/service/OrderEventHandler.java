package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.dto.OrderAggregate;
import com.mtx.trade.pipeline.dto.OrderEventMessage;
import com.mtx.trade.storage.api.StorageKey;
import com.mtx.trade.storage.api.StorageMetadata;
import com.mtx.trade.storage.api.StorageReader;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Arrays;

/** 单个订单事件处理入口；Storage 读取和 Pipeline 写事务保持分离。 */
@Service
@RequiredArgsConstructor
public class OrderEventHandler {

    private final StorageReader storageReader;
    private final FuiouOrderParser fuiouOrderParser;
    private final OrderPersistService orderPersistService;

    public OrderPersistResult handle(OrderEventMessage event) {
        String stage = OrderEventProcessStage.STORAGE_METADATA;
        try {
            StorageKey storageKey = new StorageKey(event.storageId(), event.storageSha256());
            StorageMetadata metadata = storageReader.getMetadata(storageKey);
            if (metadata == null) {
                throw new BusinessException(ErrorCode.NOT_FOUND, "订单事件关联的 Storage 元数据不存在");
            }
            if (metadata.storageId() != event.storageId()
                    || metadata.sourceSystem() != event.sourceSystem()
                    || metadata.contentType() != event.contentType()
                    || !Arrays.equals(metadata.sha256(), event.storageSha256())) {
                throw new BusinessException(ErrorCode.PARAM_INVALID, "订单事件与 Storage 元数据不一致");
            }
            stage = OrderEventProcessStage.STORAGE_CONTENT;
            byte[] content = storageReader.getContent(storageKey);
            if (content == null || content.length == 0) {
                throw new BusinessException(ErrorCode.NOT_FOUND, "订单事件关联的 Storage 原文不存在");
            }
            StorageContentValidator.requireComplete(metadata, content, "订单");
            stage = OrderEventProcessStage.PAYLOAD_PARSE;
            OrderAggregate aggregate = fuiouOrderParser.parse(content, event);
            stage = OrderEventProcessStage.ORDER_PERSIST;
            try {
                return orderPersistService.persist(aggregate);
            } catch (ConcurrentOrderInsertException firstInsertRace) {
                // 第一次事务已由 Spring 回滚；立即开启新事务重读并按版本更新，避免等待下一轮消息投递。
                return orderPersistService.persist(aggregate);
            }
        } catch (OrderEventProcessingException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new OrderEventProcessingException(stage, e);
        }
    }
}
