package com.mtx.trade.ingress.controller;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.dto.EventAckCommand;
import com.mtx.trade.ingress.dto.EventDeliveryVO;
import com.mtx.trade.ingress.dto.EventRedeliveryCommand;
import com.mtx.trade.ingress.service.EventAckService;
import com.mtx.trade.ingress.service.EventDeliveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Ingress 事件 ACK 与人工补发接口。 */
@RestController
@RequestMapping("/event")
@RequiredArgsConstructor
@Slf4j
public class EventDeliveryController {

    private final EventAckService eventAckService;
    private final EventDeliveryService eventDeliveryService;

    @PostMapping("/ack")
    public ResponseData<String> ack(@RequestBody EventAckCommand command) {
        try {
            eventAckService.ack(
                    command == null ? null : command.contentType(),
                    command == null ? null : command.eventId());
            return ResponseData.success("acked");
        } catch (BusinessException e) {
            return ResponseData.fail(e.getCode(), e.getMessage(), null);
        } catch (Exception e) {
            log.error("event ACK failed, command={}", command, e);
            return ResponseData.fail(ErrorCode.SYSTEM_ERROR);
        }
    }

    @GetMapping("/redelivery-exhausted")
    public ResponseData<List<EventDeliveryVO>> redeliveryExhausted(
            @RequestParam Integer contentType,
            @RequestParam(defaultValue = "100") Integer limit) {
        try {
            return ResponseData.success(eventDeliveryService.listExhausted(
                    contentType, limit == null ? 100 : limit));
        } catch (BusinessException e) {
            return ResponseData.fail(e.getCode(), e.getMessage(), null);
        } catch (Exception e) {
            log.error("query exhausted event failed, contentType={}", contentType, e);
            return ResponseData.fail(ErrorCode.SYSTEM_ERROR);
        }
    }

    @PostMapping("/redeliver")
    public ResponseData<String> redeliver(@RequestBody EventRedeliveryCommand command) {
        try {
            eventDeliveryService.manualRedeliver(
                    command == null ? null : command.contentType(),
                    command == null ? null : command.eventId());
            return ResponseData.success("redelivered");
        } catch (BusinessException e) {
            return ResponseData.fail(e.getCode(), e.getMessage(), null);
        } catch (Exception e) {
            log.error("manual event redelivery failed, command={}", command, e);
            return ResponseData.fail(ErrorCode.SYSTEM_ERROR);
        }
    }

    @PostMapping("/resume-redelivery")
    public ResponseData<String> resumeRedelivery(@RequestBody EventRedeliveryCommand command) {
        try {
            eventDeliveryService.resumeAutoRedelivery(
                    command == null ? null : command.contentType(),
                    command == null ? null : command.eventId());
            return ResponseData.success("resumed");
        } catch (BusinessException e) {
            return ResponseData.fail(e.getCode(), e.getMessage(), null);
        } catch (Exception e) {
            log.error("resume event redelivery failed, command={}", command, e);
            return ResponseData.fail(ErrorCode.SYSTEM_ERROR);
        }
    }
}
