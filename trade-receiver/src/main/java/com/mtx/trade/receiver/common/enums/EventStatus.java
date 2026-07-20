package com.mtx.trade.receiver.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 事件状态（trade_order_event.event_status / trade_payment_event.event_status）。
 */
@Getter
@AllArgsConstructor
public enum EventStatus {

    PENDING(0, "待执行"),
    SUCCESS(1, "执行成功"),
    FAILED(2, "执行失败"),
    IGNORED(3, "忽略");

    private final int code;
    private final String desc;
}
