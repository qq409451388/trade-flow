package com.mtx.trade.receiver.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 来源系统。
 */
@Getter
@AllArgsConstructor
public enum SourceSystem {

    UNKNOWN(0, "未知"),
    FUIOU(1, "富友");

    private final int code;
    private final String desc;
}
