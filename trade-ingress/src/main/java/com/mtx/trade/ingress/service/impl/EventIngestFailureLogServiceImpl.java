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

import java.util.HashSet;
import java.util.Set;

/** Event 接入失败审计服务实现。 */
@Service
@RequiredArgsConstructor
public class EventIngestFailureLogServiceImpl implements EventIngestFailureLogService {

    private static final int MAX_REASON_LENGTH = 1024;
    private static final int MAX_CAUSE_DEPTH = 8;

    private final EventIngestFailureLogDbService failureLogDbService;

    @Override
    @Transactional(
            transactionManager = "ingressTransactionManager",
            propagation = Propagation.REQUIRES_NEW,
            rollbackFor = Exception.class)
    public Long recordFailure(
            String requestId,
            int sourceSystem,
            int contentType,
            byte[] payloadSha256,
            StorageRef storageRef,
            String failureStage,
            ParsedEventVersion parsedEvent,
            Throwable failure) {
        if (!StringUtils.hasText(requestId)) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "event接入失败审计requestId不能为空");
        }
        if (payloadSha256 == null || payloadSha256.length != 32) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "event接入失败审计payloadSha256必须为32字节");
        }
        EventIngestFailureLogDO entity = new EventIngestFailureLogDO();
        entity.setRequestId(requestId);
        entity.setSourceSystem(sourceSystem);
        entity.setContentType(contentType);
        entity.setRawId(storageRef == null ? null : storageRef.storageId());
        entity.setPayloadSha256(payloadSha256);
        entity.setFailureStage(failureStage);
        entity.setErrorCode(failure instanceof BusinessException businessException
                ? businessException.getCode() : ErrorCode.SYSTEM_ERROR.getCode());
        entity.setExceptionType(failure == null ? "unknown" : failure.getClass().getName());
        entity.setFailureReason(limitReason(failure));
        if (parsedEvent != null) {
            entity.setThirdEventKey(parsedEvent.eventKey());
            entity.setMessageVersion(parsedEvent.messageVersion());
        }
        if (!failureLogDbService.save(entity)) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "event接入失败审计记录保存失败");
        }
        return entity.getId();
    }

    private static String limitReason(Throwable failure) {
        if (failure == null) {
            return "unknown";
        }
        StringBuilder reason = new StringBuilder();
        Set<Throwable> visited = new HashSet<>();
        Throwable current = failure;
        int depth = 0;
        while (current != null && depth < MAX_CAUSE_DEPTH && visited.add(current)) {
            if (!reason.isEmpty()) {
                reason.append(" <- ");
            }
            reason.append(current.getClass().getSimpleName());
            if (StringUtils.hasText(current.getMessage())) {
                reason.append(": ").append(current.getMessage());
            }
            current = current.getCause();
            depth++;
        }
        return reason.length() <= MAX_REASON_LENGTH
                ? reason.toString()
                : reason.substring(0, MAX_REASON_LENGTH);
    }
}
