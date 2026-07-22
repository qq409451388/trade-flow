package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.dto.PaymentAggregate;
import com.mtx.trade.pipeline.dto.PaymentEventMessage;
import com.mtx.trade.storage.api.StorageKey;
import com.mtx.trade.storage.api.StorageMetadata;
import com.mtx.trade.storage.api.StorageReader;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Arrays;

/** 单个支付事件处理入口；Storage 读取与支付数据库事务分离。 */
@Service
@RequiredArgsConstructor
public class PaymentEventHandler {

    private final StorageReader storageReader;
    private final FuiouPaymentParser paymentParser;
    private final PaymentPersistService paymentPersistService;

    public PaymentPersistResult handle(PaymentEventMessage event) {
        String stage = PaymentEventProcessStage.STORAGE_METADATA;
        try {
            StorageKey storageKey = new StorageKey(event.storageId(), event.storageSha256());
            StorageMetadata metadata = storageReader.getMetadata(storageKey);
            if (metadata == null) {
                throw new BusinessException(ErrorCode.NOT_FOUND, "支付事件关联的 Storage 元数据不存在");
            }
            if (metadata.storageId() != event.storageId()
                    || metadata.sourceSystem() != event.sourceSystem()
                    || metadata.contentType() != event.contentType()
                    || !Arrays.equals(metadata.sha256(), event.storageSha256())) {
                throw new BusinessException(ErrorCode.PARAM_INVALID, "支付事件与 Storage 元数据不一致");
            }
            stage = PaymentEventProcessStage.STORAGE_CONTENT;
            byte[] content = storageReader.getContent(storageKey);
            if (content == null || content.length == 0) {
                throw new BusinessException(ErrorCode.NOT_FOUND, "支付事件关联的 Storage 原文不存在");
            }
            StorageContentValidator.requireComplete(metadata, content, "支付");
            stage = PaymentEventProcessStage.PAYLOAD_PARSE;
            PaymentAggregate aggregate = paymentParser.parse(content, event, metadata.receivedTime());
            stage = PaymentEventProcessStage.PAYMENT_PERSIST;
            try {
                return paymentPersistService.persist(aggregate);
            } catch (ConcurrentPaymentInsertException firstInsertRace) {
                // 原事务已经回滚，新事务重读后按 SHA 判断幂等或冲突。
                return paymentPersistService.persist(aggregate);
            }
        } catch (PaymentEventProcessingException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new PaymentEventProcessingException(stage, e);
        }
    }
}
