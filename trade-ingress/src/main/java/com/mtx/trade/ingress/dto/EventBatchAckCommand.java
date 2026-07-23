package com.mtx.trade.ingress.dto;

import java.util.List;

/** Pipeline 批量确认已经处理完成或进入人工终态的事件。 */
public record EventBatchAckCommand(Integer contentType, List<Long> eventIds) {
}
