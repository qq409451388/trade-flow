package com.mtx.trade.pipeline.exception;

/** 携带明确处理阶段的订单事件异常。 */
public class OrderEventProcessingException extends RuntimeException {

    private final String stage;

    public OrderEventProcessingException(String stage, Throwable cause) {
        super(cause == null ? null : cause.getMessage(), cause);
        this.stage = stage;
    }

    public String getStage() {
        return stage;
    }
}
