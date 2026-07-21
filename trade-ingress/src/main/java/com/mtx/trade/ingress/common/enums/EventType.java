package com.mtx.trade.ingress.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 事件类型（trade_event_execution_log.event_type）。
 */
@Getter
@AllArgsConstructor
public enum EventType {

    ORDER(1, "订单"),
    PAYMENT(2, "支付");

    private final int code;
    private final String desc;
}
