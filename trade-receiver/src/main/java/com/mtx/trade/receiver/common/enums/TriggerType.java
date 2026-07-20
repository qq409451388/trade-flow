package com.mtx.trade.receiver.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 触发方式（trade_event_execution_log.trigger_type）。
 */
@Getter
@AllArgsConstructor
public enum TriggerType {

    FIRST_CONSUME(1, "首次消费"),
    AUTO_RETRY(2, "自动重试"),
    MANUAL_RERUN(3, "人工重跑"),
    BATCH_REPLAY(4, "批量回放");

    private final int code;
    private final String desc;
}
