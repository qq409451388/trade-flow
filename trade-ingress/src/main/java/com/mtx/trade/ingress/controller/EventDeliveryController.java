package com.mtx.trade.ingress.controller;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.dto.EventDeliveryVO;
import com.mtx.trade.ingress.service.EventDeliveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Ingress Pipeline内部未 ACK 事件查询接口。 */
@RestController
@RequestMapping("/event")
@RequiredArgsConstructor
@Slf4j
public class EventDeliveryController {

    private final EventDeliveryService eventDeliveryService;

    @GetMapping("/unacked")
    public ResponseData<List<EventDeliveryVO>> unacked(
            @RequestParam Integer contentType,
            @RequestParam(defaultValue = "100") Integer limit,
            @RequestParam(required = false) List<Long> eventIds,
            @RequestParam(defaultValue = "0") Long afterEventId) {
        try {
            return ResponseData.success(eventDeliveryService.listUnacked(
                    contentType, limit == null ? 100 : limit, eventIds,
                    afterEventId == null ? 0L : afterEventId));
        } catch (BusinessException e) {
            return ResponseData.fail(e.getCode(), e.getMessage(), null);
        } catch (Exception e) {
            log.error("[Unacked Event Pull] ❌ Unacknowledged-event query failed. contentType={}", contentType, e);
            return ResponseData.fail(ErrorCode.SYSTEM_ERROR);
        }
    }
}
