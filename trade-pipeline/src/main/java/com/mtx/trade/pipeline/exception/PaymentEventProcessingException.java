package com.mtx.trade.pipeline.exception;

import lombok.Getter;

/** 携带支付事件失败阶段，供独立审计事务记录。 */
@Getter
public class PaymentEventProcessingException extends RuntimeException {

    private final String stage;

    public PaymentEventProcessingException(String stage, Throwable cause) {
        super(cause == null ? null : cause.getMessage(), cause);
        this.stage = stage;
    }
}
