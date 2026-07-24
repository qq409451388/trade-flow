package com.mtx.trade.pipeline.event.processor;

import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.dto.OrderAggregate;
import com.mtx.trade.pipeline.dto.OrderEventMessage;
import com.mtx.trade.pipeline.enums.OrderPersistResult;
import com.mtx.trade.pipeline.exception.ConcurrentOrderInsertException;
import com.mtx.trade.pipeline.exception.OrderEventProcessingException;
import com.mtx.trade.pipeline.service.*;
import com.mtx.trade.storage.api.StorageKey;
import com.mtx.trade.storage.api.StorageMetadata;
import com.mtx.trade.storage.api.StorageReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.stereotype.Service;

import java.util.Arrays;

/** 单个订单事件处理入口；Storage 读取和 Pipeline 写事务保持分离。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderEventHandler {

    private static final int MAX_PERSIST_ATTEMPTS = 3;
    private static final long DEADLOCK_RETRY_BACKOFF_MS = 20L;

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
            return persistWithTransientRetry(aggregate);
        } catch (OrderEventProcessingException e) {
            log.error("handle order processing ex:{}", e.getMessage(), e);
            throw e;
        } catch (RuntimeException e) {
            log.error("handle order runtime ex:{}", e.getMessage(), e);
            throw new OrderEventProcessingException(stage, e);
        }
    }

    /** 每次调用都会经过 OrderPersistService 事务代理，因此死锁后会在全新事务中完整重试。 */
    private OrderPersistResult persistWithTransientRetry(OrderAggregate aggregate) {
        for (int attempt = 1; attempt <= MAX_PERSIST_ATTEMPTS; attempt++) {
            try {
                return orderPersistService.persist(aggregate);
            } catch (ConcurrentOrderInsertException insertRace) {
                if (attempt == MAX_PERSIST_ATTEMPTS) throw insertRace;
                log.debug("[Order Persistence] 🔄 Concurrent first insert will be retried in a new transaction. "
                        + "orderNo={}, attempt={}", aggregate.order().getOrderNo(), attempt);
            } catch (PessimisticLockingFailureException deadlock) {
                if (attempt == MAX_PERSIST_ATTEMPTS) throw deadlock;
                log.warn("[Order Persistence] 🔄 MySQL deadlock will be retried in a new transaction. "
                        + "orderNo={}, attempt={}, maxAttempts={}",
                        aggregate.order().getOrderNo(), attempt, MAX_PERSIST_ATTEMPTS);
                backoffAfterDeadlock(attempt, deadlock);
            }
        }
        throw new IllegalStateException("订单事务重试未返回结果");
    }

    private static void backoffAfterDeadlock(int attempt, RuntimeException deadlock) {
        try {
            Thread.sleep(DEADLOCK_RETRY_BACKOFF_MS * attempt);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            deadlock.addSuppressed(interrupted);
            throw deadlock;
        }
    }
}
