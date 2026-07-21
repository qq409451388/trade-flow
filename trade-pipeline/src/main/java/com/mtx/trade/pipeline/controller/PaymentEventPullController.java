package com.mtx.trade.pipeline.controller;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.dto.PaymentEventPullCommand;
import com.mtx.trade.pipeline.dto.PaymentEventPullResult;
import com.mtx.trade.pipeline.service.PaymentEventPullService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Pipeline 主动恢复投递耗尽的支付事件。 */
@Slf4j
@RestController
@RequestMapping("/payment-event")
@RequiredArgsConstructor
public class PaymentEventPullController {

    private final PaymentEventPullService paymentEventPullService;

    @PostMapping("/pull")
    public ResponseData<List<PaymentEventPullResult>> pull(
            @RequestBody(required = false) PaymentEventPullCommand command) {
        try {
            return ResponseData.success(paymentEventPullService.pull(command));
        } catch (BusinessException e) {
            return ResponseData.fail(e.getCode(), e.getMessage(), null);
        } catch (Exception e) {
            log.error("actively pull exhausted payment events failed", e);
            return ResponseData.fail(ErrorCode.SYSTEM_ERROR);
        }
    }
}
