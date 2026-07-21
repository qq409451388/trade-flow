package com.mtx.trade.receiver.controller;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.receiver.common.enums.SourceSystem;
import com.mtx.trade.receiver.dto.EventIngestResult;
import com.mtx.trade.receiver.dto.FuiouResponse;
import com.mtx.trade.receiver.dto.ParsedEventVersion;
import com.mtx.trade.receiver.entity.OrderEventDO;
import com.mtx.trade.receiver.service.EventStreamPublisher;
import com.mtx.trade.receiver.service.FuiouOrderPayloadParser;
import com.mtx.trade.receiver.service.OrderEventService;
import com.mtx.trade.receiver.utils.FuiouSignUtils;
import com.mtx.trade.storage.api.*;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

@Slf4j
@RestController
@RequestMapping("/order")
public class OrderController {
    @Resource
    private StorageReader storageReader;
    @Resource
    private StorageWriter storageWriter;
    @Resource
    private OrderEventService orderEventService;
    @Resource
    private EventStreamPublisher eventStreamPublisher;
    @Resource
    private FuiouOrderPayloadParser fuiouOrderPayloadParser;
    @Value("${trade.thirdparty.fuiou.secret}")
    private String secret;

    @GetMapping("/storage-metadata")
    public ResponseData<StorageMetadata> storageMetadata(@RequestParam Long storageId) {
        return ResponseData.success(storageReader.getMetadata(storageId));
    }
    @PostMapping("/store-push")
    public FuiouResponse storePush(@RequestBody String payload) {
        OrderEventDO eventDO = null;
        try {
            FuiouSignUtils.FuiouSignParts fuiouSignParts = FuiouSignUtils.parseSign(payload);
            boolean verifyResult = FuiouSignUtils.verifySign(fuiouSignParts, secret);
            if (!verifyResult) {
                return FuiouResponse.fail("verify sign failed.");
            }
            StorageRef storageRef = this.writeStorage(payload);
            eventDO = this.createEvent(payload, storageRef);
            return FuiouResponse.ok();
        } catch (StorageWriteException e) {
            log.warn("storage write failed", e);
            return FuiouResponse.fail(ErrorCode.DATA_CREATE_ERROR.getMessage());
        } catch (BusinessException e) {
            return FuiouResponse.fail(e.getMessage());
        } catch (Exception e) {
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
        return storageWriter.put(storageWriteCommand);
    }

    private OrderEventDO createEvent(String payload, StorageRef storageRef) {
        OrderEventDO eventDO = null;
        ParsedEventVersion parsedEvent = fuiouOrderPayloadParser.parse(payload);
        EventIngestResult<OrderEventDO> ingestResult = orderEventService.createEvent(
                SourceSystem.FUIOU.getCode(),
                parsedEvent.eventKey(),
                parsedEvent.messageVersion(),
                storageRef.storageId(),
                storageRef.sha256());
        if (ingestResult.accepted()) {
            eventDO = ingestResult.event();
        }
        return eventDO;
    }

    private void publishOrderEvent(OrderEventDO eventDO) {
        if (eventDO != null) {
            try {
                eventStreamPublisher.publishOrderEvent(eventDO);
            } catch (Exception e) {
                log.warn("stream publish failed, eventId={}", eventDO.getId(), e);
            }
        }
    }
}
