package com.mtx.trade.pipeline.enums;

/** 支付事件处理失败阶段。 */
public final class PaymentEventProcessStage {

    public static final String STREAM_MESSAGE = "STREAM_MESSAGE";
    public static final String STORAGE_METADATA = "STORAGE_METADATA";
    public static final String STORAGE_CONTENT = "STORAGE_CONTENT";
    public static final String PAYLOAD_PARSE = "PAYLOAD_PARSE";
    public static final String PAYMENT_PERSIST = "PAYMENT_PERSIST";

    private PaymentEventProcessStage() {
    }
}
