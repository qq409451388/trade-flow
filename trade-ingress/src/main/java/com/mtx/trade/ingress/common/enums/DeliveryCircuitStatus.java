package com.mtx.trade.ingress.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** Redis Stream 事件投递熔断状态。 */
@Getter
@AllArgsConstructor
public enum DeliveryCircuitStatus {

    CLOSED(0),
    OPEN(1),
    HALF_OPEN(2);

    private final int code;
}
