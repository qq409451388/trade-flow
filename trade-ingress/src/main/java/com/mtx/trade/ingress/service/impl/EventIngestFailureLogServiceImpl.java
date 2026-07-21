package com.mtx.trade.ingress.service.impl;

import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.dto.ParsedEventVersion;
import com.mtx.trade.ingress.entity.EventIngestFailureLogDO;
import com.mtx.trade.ingress.service.EventIngestFailureLogService;
import com.mtx.trade.ingress.service.db.EventIngestFailureLogDbService;
import com.mtx.trade.storage.api.StorageRef;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

/** Event 接入失败审计服务实现。 */
@Service
@RequiredArgsConstructor
public class EventIngestFailureLogServiceImpl implements EventIngestFailureLogService {

    private static final int MAX_REASON_LENGTH = 1024;

    private final EventIngestFailureLogDbService failureLogDbService;

    @Override
    @Transactional(
            transactionManager = "ingressTransactionManager",
            propagation = Propagation.REQUIRES_NEW,
            rollbackFor = Exception.class)
    public void recordFailure(
            int sourceSystem,
            int contentType,
            StorageRef storageRef,
            String failureStage,
            ParsedEventVersion parsedEvent,
            Throwable failure) {
        if (storageRef == null || storageRef.storageId() == null || storageRef.sha256() == null) {
            return;
        }
        EventIngestFailureLogDO entity = new EventIngestFailureLogDO();
        entity.setSourceSystem(sourceSystem);
        entity.setContentType(contentType);
        entity.setRawId(storageRef.storageId());
        entity.setPayloadSha256(storageRef.sha256());
        entity.setFailureStage(failureStage);
        entity.setErrorCode(failure instanceof BusinessException businessException
                ? businessException.getCode() : ErrorCode.SYSTEM_ERROR.getCode());
        entity.setFailureReason(limitReason(failure));
        if (parsedEvent != null) {
            entity.setThirdEventKey(parsedEvent.eventKey());
            entity.setMessageVersion(parsedEvent.messageVersion());
        }
        if (!failureLogDbService.save(entity)) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "event接入失败审计记录保存失败");
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
