package com.mtx.trade.pipeline.service;

/** 订单事件处理失败阶段。 */
public final class OrderEventProcessStage {

    public static final String STREAM_MESSAGE = "STREAM_MESSAGE";
    public static final String STORAGE_METADATA = "STORAGE_METADATA";
    public static final String STORAGE_CONTENT = "STORAGE_CONTENT";
    public static final String PAYLOAD_PARSE = "PAYLOAD_PARSE";
    public static final String ORDER_PERSIST = "ORDER_PERSIST";
    public static final String INGRESS_ACK = "INGRESS_ACK";

    private OrderEventProcessStage() {
    }
}
