package com.mtx.trade.pipeline.dto;

import java.util.List;

/** 主动拉取支付事件，可指定 event ID 或限制批量数量。 */
public record PaymentEventPullCommand(List<Long> eventIds, Integer limit) {
}
