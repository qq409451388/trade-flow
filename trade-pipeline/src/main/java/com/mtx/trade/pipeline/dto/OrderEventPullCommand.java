package com.mtx.trade.pipeline.dto;

import java.util.List;

/** Pipeline 主动拉取订单事件命令；eventIds为空时按limit拉取最旧耗尽事件。 */
public record OrderEventPullCommand(List<Long> eventIds, Integer limit) {
}
