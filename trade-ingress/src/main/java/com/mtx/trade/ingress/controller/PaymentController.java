package com.mtx.trade.ingress.controller;

import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.common.enums.SourceSystem;
import com.mtx.trade.ingress.dto.EventIngestResult;
import com.mtx.trade.ingress.dto.FuiouResponse;
import com.mtx.trade.ingress.dto.ParsedEventVersion;
import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.entity.PaymentEventDO;
import com.mtx.trade.ingress.service.EventDeliveryService;
import com.mtx.trade.ingress.service.FuiouOrderPayloadParser;
import com.mtx.trade.ingress.service.PaymentEventService;
import com.mtx.trade.ingress.service.StorageWriteService;
import com.mtx.trade.ingress.utils.FuiouSignUtils;
import com.mtx.trade.storage.api.*;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HexFormat;

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
    private FuiouOrderPayloadParser fuiouOrderPayloadParser;
    @Value("${trade.thirdparty.fuiou.secret}")
    private String secret;

    @PostMapping("/store-push")
    public FuiouResponse storePush(@RequestBody String payload) {
        PaymentEventDO eventDO = null;
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
            this.publishPaymentEvent(eventDO);
        }
    }

    private StorageRef writeStorage(String payload) {
        StorageWriteCommand storageWriteCommand = new StorageWriteCommand(
                SourceSystem.FUIOU.getCode(),
                ContentType.PAYMENT.getCode(), payload.getBytes(StandardCharsets.UTF_8), LocalDateTime.now()
        );
        return storageWriteService.putIfAbsent(storageWriteCommand);
    }

    private PaymentEventDO createEvent(String payload, StorageRef storageRef) {
        PaymentEventDO eventDO = null;
        ParsedEventVersion parsedEvent = fuiouOrderPayloadParser.parse(payload);
        EventIngestResult<PaymentEventDO> ingestResult = paymentEventService.createEvent(
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

    private void publishPaymentEvent(PaymentEventDO eventDO) {
        if (eventDO != null) {
            try {
                eventDeliveryService.deliverPaymentEvent(eventDO);
            } catch (Exception e) {
                log.warn("stream publish failed, eventId={}", eventDO.getId(), e);
            }
        }
    }

    private static byte[] parseSha256(String value) {
        try {
            if (value == null || value.length() != 64) {
                throw new IllegalArgumentException("invalid length");
            }
            return HexFormat.of().parseHex(value);
        } catch (IllegalArgumentException e) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "storageSha256 必须为64位十六进制字符串");
        }
    }
}
