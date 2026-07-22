package com.mtx.trade.ingress.controller;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.dto.EventAckCommand;
import com.mtx.trade.ingress.service.EventAckService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Pipeline 接管事件后的 Ingress ACK 接口。 */
@RestController
@RequestMapping("/event")
@RequiredArgsConstructor
@Slf4j
public class EventAckController {

    private final EventAckService eventAckService;

    @PostMapping("/ack")
    public ResponseData<String> ack(@RequestBody EventAckCommand command) {
        try {
            boolean newlyAcknowledged = eventAckService.ack(
                    command == null ? null : command.contentType(),
                    command == null ? null : command.eventId());
            if (newlyAcknowledged) {
                log.debug("[Ingress ACK] ✅ Event acknowledged by Pipeline for the first time. "
                                + "contentType={}, eventId={}",
                        command == null ? null : command.contentType(),
                        command == null ? null : command.eventId());
            } else {
                log.debug("[Ingress ACK] ✅ Duplicate ACK accepted idempotently; event was already acknowledged. "
                                + "contentType={}, eventId={}",
                        command == null ? null : command.contentType(),
                        command == null ? null : command.eventId());
            }
            return ResponseData.success("acked");
        } catch (BusinessException e) {
            return ResponseData.fail(e.getCode(), e.getMessage(), null);
        } catch (Exception e) {
            log.error("[Ingress ACK] ❌ Event acknowledgement failed; the event remains unacknowledged. "
                    + "command={}", command, e);
            return ResponseData.fail(ErrorCode.SYSTEM_ERROR);
        }
    }
}
