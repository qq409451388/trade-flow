package com.mtx.trade.pipeline.controller;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.dto.OrderEventPullCommand;
import com.mtx.trade.pipeline.dto.OrderEventPullResult;
import com.mtx.trade.pipeline.service.OrderEventPullService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Pipeline 主动恢复投递耗尽订单事件。 */
@Slf4j
@RestController
@RequestMapping("/order-event")
@RequiredArgsConstructor
public class OrderEventPullController {

    private final OrderEventPullService orderEventPullService;

    @PostMapping("/pull")
    public ResponseData<List<OrderEventPullResult>> pull(@RequestBody(required = false) OrderEventPullCommand command) {
        try {
            return ResponseData.success(orderEventPullService.pull(command));
        } catch (BusinessException e) {
            return ResponseData.fail(e.getCode(), e.getMessage(), null);
        } catch (Exception e) {
            log.error("actively pull exhausted order events failed", e);
            return ResponseData.fail(ErrorCode.SYSTEM_ERROR);
        }
    }
}
