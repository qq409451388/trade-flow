package com.mtx.trade.pipeline.dto;

import java.util.List;

/** 主动拉取支付事件，可指定 event ID，或从 afterEventId 后按limit拉取。 */
public record PaymentEventPullCommand(List<Long> eventIds, Integer limit, Long afterEventId) {
}
