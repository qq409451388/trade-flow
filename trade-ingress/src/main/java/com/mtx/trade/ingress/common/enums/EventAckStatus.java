package com.mtx.trade.ingress.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** Ingress 事件投递确认状态。 */
@Getter
@AllArgsConstructor
public enum EventAckStatus {

    INIT(0, "等待 Pipeline ACK"),
    ACKED(1, "Pipeline 已接管");

    private final int code;
    private final String desc;
}
