package com.mtx.trade.receiver.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 执行结果（trade_event_execution_log.execution_status）。
 */
@Getter
@AllArgsConstructor
public enum ExecutionStatus {

    SUCCESS(1, "成功"),
    FAILED(2, "失败"),
    IGNORED(3, "忽略");

    private final int code;
    private final String desc;
}
