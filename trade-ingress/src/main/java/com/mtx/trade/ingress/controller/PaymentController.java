package com.mtx.trade.ingress.controller;

import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.common.enums.EventAckStatus;
import com.mtx.trade.ingress.common.enums.SourceSystem;
import com.mtx.trade.ingress.dto.EventIngestResult;
import com.mtx.trade.ingress.dto.FuiouResponse;
import com.mtx.trade.ingress.dto.IngestRequestTrace;
import com.mtx.trade.ingress.dto.ParsedEventVersion;
import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.entity.PaymentEventDO;
import com.mtx.trade.ingress.service.*;
import com.mtx.trade.ingress.utils.FuiouSignUtils;
import com.mtx.trade.storage.api.*;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.Objects;

@Slf4j
@RestController
@RequestMapping("/payment")
public class PaymentController {
    @Resource
    private StorageWriteService storageWriteService;
    @Resource
    private PaymentEventService paymentEventService;
    @Resource
    private EventDeliveryService eventDeliveryService;
    @Resource
    private FuiouPaymentPayloadParser fuiouPaymentPayloadParser;
    @Resource
    private EventIngestFailureLogService eventIngestFailureLogService;
    @Resource
    private IngressRequestMonitor ingressRequestMonitor;
    @Value("${trade.thirdparty.fuiou.secret}")
    private String secret;

    @PostMapping("/store-push")
    public FuiouResponse storePush(@RequestBody String payload) {
        long requestStartedNanos = System.nanoTime();
        long signatureNanos = 0;
        long storageNanos = 0;
        long eventParseNanos = 0;
        long eventPersistNanos = 0;
        IngestRequestTrace trace = IngestRequestTrace.fromPayload(payload);
        PaymentEventDO eventDO = null;
        StorageRef storageRef = null;
        ParsedEventVersion parsedEvent = null;
        String failureStage = EventIngestFailureLogService.STAGE_SIGNATURE_VERIFY;
        try {
            long stageStartedNanos = System.nanoTime();
            FuiouSignUtils.FuiouSignParts fuiouSignParts;
            boolean verifyResult;
            try {
                fuiouSignParts = FuiouSignUtils.parseSign(payload);
                verifyResult = FuiouSignUtils.verifySign(fuiouSignParts, secret);
            } finally {
                signatureNanos = System.nanoTime() - stageStartedNanos;
            }
            if (!verifyResult) {
                BusinessException failure = new BusinessException(
                        ErrorCode.FORBIDDEN, "verify sign failed.");
                Long auditId = this.recordEventFailure(
                        trace, storageRef, failureStage, parsedEvent, failure);
                log.warn("[Ingress] Payment push rejected. requestId={}, payloadSha256={}, "
                                + "stage={}, auditId={}, reason={}",
                        trace.requestId(), trace.payloadSha256Hex(), failureStage,
                        auditId, failure.getMessage());
                return failureResponse(failure.getMessage(), trace.requestId());
            }
            failureStage = EventIngestFailureLogService.STAGE_STORAGE_PERSIST;
            stageStartedNanos = System.nanoTime();
            try {
                storageRef = this.writeStorage(payload);
            } finally {
                storageNanos = System.nanoTime() - stageStartedNanos;
            }
            failureStage = EventIngestFailureLogService.STAGE_EVENT_FIELD_PARSE;
            stageStartedNanos = System.nanoTime();
            try {
                parsedEvent = fuiouPaymentPayloadParser.parse(payload);
            } finally {
                eventParseNanos = System.nanoTime() - stageStartedNanos;
            }
            failureStage = EventIngestFailureLogService.STAGE_EVENT_PERSIST;
            stageStartedNanos = System.nanoTime();
            try {
                eventDO = this.createEvent(parsedEvent, storageRef);
            } finally {
                eventPersistNanos = System.nanoTime() - stageStartedNanos;
            }
            return FuiouResponse.ok();
        } catch (StorageWriteException e) {
            Long auditId = this.recordEventFailure(
                    trace, storageRef, failureStage, parsedEvent, e);
            log.error("[Storage] ❌ Payment payload persistence failed; the request was rejected. "
                            + "requestId={}, payloadSha256={}, stage={}, auditId={}",
                    trace.requestId(), trace.payloadSha256Hex(), failureStage, auditId, e);
            return failureResponse(ErrorCode.DATA_CREATE_ERROR.getMessage(), trace.requestId());
        } catch (BusinessException e) {
            Long auditId = this.recordEventFailure(
                    trace, storageRef, failureStage, parsedEvent, e);
            log.warn("[Ingress] Payment push business validation failed. requestId={}, "
                            + "payloadSha256={}, stage={}, auditId={}, reason={}",
                    trace.requestId(), trace.payloadSha256Hex(), failureStage,
                    auditId, e.getMessage());
            return failureResponse(e.getMessage(), trace.requestId());
        } catch (Exception e) {
            Long auditId = this.recordEventFailure(
                    trace, storageRef, failureStage, parsedEvent, e);
            log.error("[Ingress] ❌ Payment push processing failed. requestId={}, "
                            + "payloadSha256={}, stage={}, auditId={}, reason={}",
                    trace.requestId(), trace.payloadSha256Hex(), failureStage,
                    auditId, e.getMessage(), e);
            return failureResponse(ErrorCode.SYSTEM_ERROR.getMessage(), trace.requestId());
        } finally {
            long publishStartedNanos = System.nanoTime();
            this.publishPaymentEvent(eventDO);
            long publishNanos = System.nanoTime() - publishStartedNanos;
            ingressRequestMonitor.logIfSlow(
                    "payment",
                    trace,
                    failureStage,
                    System.nanoTime() - requestStartedNanos,
                    signatureNanos,
                    storageNanos,
                    eventParseNanos,
                    eventPersistNanos,
                    publishNanos);
        }
    }

    private StorageRef writeStorage(String payload) {
        StorageWriteCommand storageWriteCommand = new StorageWriteCommand(
                SourceSystem.FUIOU.getCode(),
                ContentType.PAYMENT.getCode(), payload.getBytes(StandardCharsets.UTF_8), LocalDateTime.now()
        );
        return storageWriteService.putIfAbsent(storageWriteCommand);
    }

    private PaymentEventDO createEvent(ParsedEventVersion parsedEvent, StorageRef storageRef) {
        PaymentEventDO eventDO = null;
        EventIngestResult<PaymentEventDO> ingestResult = paymentEventService.createEvent(
                SourceSystem.FUIOU.getCode(),
                parsedEvent.eventKey(),
                parsedEvent.messageVersion(),
                storageRef.storageId(),
                storageRef.sha256());
        if (ingestResult.shouldPublish(event -> Objects.equals(
                event.getAcked(), EventAckStatus.INIT.getCode()))) {
            eventDO = ingestResult.event();
        }
        return eventDO;
    }

    private Long recordEventFailure(
            IngestRequestTrace trace,
            StorageRef storageRef,
            String failureStage,
            ParsedEventVersion parsedEvent,
            Throwable failure) {
        try {
            return eventIngestFailureLogService.recordFailure(
                    trace.requestId(), SourceSystem.FUIOU.getCode(), ContentType.PAYMENT.getCode(),
                    trace.payloadSha256(), storageRef,
                    failureStage, parsedEvent, failure);
        } catch (Exception auditException) {
            log.error("[Ingress Audit] ❌ Payment ingest failure could not be recorded. "
                            + "requestId={}, payloadSha256={}, storageId={}, stage={}",
                    trace.requestId(), trace.payloadSha256Hex(),
                    storageRef == null ? null : storageRef.storageId(),
                    failureStage, auditException);
            return null;
        }
    }

    private static FuiouResponse failureResponse(String message, String requestId) {
        return FuiouResponse.fail(message + "; requestId=" + requestId);
    }

    private void publishPaymentEvent(PaymentEventDO eventDO) {
        if (eventDO != null) {
            try {
                eventDeliveryService.deliverPaymentEvent(eventDO);
            } catch (Exception e) {
                log.error("[Redis Stream] ❌ Payment publish orchestration failed; the persisted event remains "
                        + "available for recovery. eventId={}", eventDO.getId(), e);
            }
        }
    }

}
