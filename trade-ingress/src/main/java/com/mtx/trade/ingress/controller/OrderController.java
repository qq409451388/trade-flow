package com.mtx.trade.ingress.controller;

import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.common.enums.EventAckStatus;
import com.mtx.trade.ingress.common.enums.SourceSystem;
import com.mtx.trade.ingress.dto.EventIngestResult;
import com.mtx.trade.ingress.dto.FuiouResponse;
import com.mtx.trade.ingress.dto.ParsedEventVersion;
import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.service.EventDeliveryService;
import com.mtx.trade.ingress.service.EventIngestFailureLogService;
import com.mtx.trade.ingress.service.FuiouOrderPayloadParser;
import com.mtx.trade.ingress.service.OrderEventService;
import com.mtx.trade.ingress.service.StorageWriteService;
import com.mtx.trade.ingress.utils.FuiouSignUtils;
import com.mtx.trade.storage.api.*;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Objects;

@Slf4j
@RestController
@RequestMapping("/order")
public class OrderController {
    @Resource
    private StorageWriteService storageWriteService;
    @Resource
    private OrderEventService orderEventService;
    @Resource
    private EventDeliveryService eventDeliveryService;
    @Resource
    private FuiouOrderPayloadParser fuiouOrderPayloadParser;
    @Resource
    private EventIngestFailureLogService eventIngestFailureLogService;
    @Value("${trade.thirdparty.fuiou.secret}")
    private String secret;

    @PostMapping("/store-push")
    public FuiouResponse storePush(@RequestBody String payload) {
        OrderEventDO eventDO = null;
        StorageRef storageRef = null;
        ParsedEventVersion parsedEvent = null;
        String failureStage = EventIngestFailureLogService.STAGE_EVENT_FIELD_PARSE;
        try {
            FuiouSignUtils.FuiouSignParts fuiouSignParts = FuiouSignUtils.parseSign(payload);
            boolean verifyResult = FuiouSignUtils.verifySign(fuiouSignParts, secret);
            if (!verifyResult) {
                return FuiouResponse.fail("verify sign failed.");
            }
            storageRef = this.writeStorage(payload);
            parsedEvent = fuiouOrderPayloadParser.parse(payload);
            failureStage = EventIngestFailureLogService.STAGE_EVENT_PERSIST;
            eventDO = this.createEvent(parsedEvent, storageRef);
            return FuiouResponse.ok();
        } catch (StorageWriteException e) {
            log.warn("storage write failed", e);
            return FuiouResponse.fail(ErrorCode.DATA_CREATE_ERROR.getMessage());
        } catch (BusinessException e) {
            this.recordEventFailure(storageRef, failureStage, parsedEvent, e);
            return FuiouResponse.fail(e.getMessage());
        } catch (Exception e) {
            this.recordEventFailure(storageRef, failureStage, parsedEvent, e);
            log.error("store push exception, msg:{}", e.getMessage(), e);
            return FuiouResponse.fail(ErrorCode.SYSTEM_ERROR.getMessage());
        } finally {
            this.publishOrderEvent(eventDO);
        }
    }

    private StorageRef writeStorage(String payload) {
        StorageWriteCommand storageWriteCommand = new StorageWriteCommand(
                SourceSystem.FUIOU.getCode(),
                ContentType.ORDER.getCode(), payload.getBytes(StandardCharsets.UTF_8), LocalDateTime.now()
        );
        return storageWriteService.putIfAbsent(storageWriteCommand);
    }

    private OrderEventDO createEvent(ParsedEventVersion parsedEvent, StorageRef storageRef) {
        OrderEventDO eventDO = null;
        EventIngestResult<OrderEventDO> ingestResult = orderEventService.createEvent(
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

    private void recordEventFailure(
            StorageRef storageRef,
            String failureStage,
            ParsedEventVersion parsedEvent,
            Throwable failure) {
        if (storageRef == null) {
            return;
        }
        try {
            eventIngestFailureLogService.recordFailure(
                    SourceSystem.FUIOU.getCode(), ContentType.ORDER.getCode(), storageRef,
                    failureStage, parsedEvent, failure);
        } catch (Exception auditException) {
            log.error("record order event ingest failure failed, storageId={}",
                    storageRef.storageId(), auditException);
        }
    }

    private void publishOrderEvent(OrderEventDO eventDO) {
        if (eventDO != null) {
            try {
                eventDeliveryService.deliverOrderEvent(eventDO);
            } catch (Exception e) {
                log.warn("stream publish failed, eventId={}", eventDO.getId(), e);
            }
        }
    }

}
