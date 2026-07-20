package com.mtx.trade.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 内容类型（trade_storage.content_type）。
 */
@Getter
@AllArgsConstructor
public enum ContentType {

    ORDER(1, "订单"),
    PAYMENT(2, "支付");

    private final int code;
    private final String desc;
}
