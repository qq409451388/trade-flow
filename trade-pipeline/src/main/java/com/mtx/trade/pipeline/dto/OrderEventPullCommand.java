package com.mtx.trade.pipeline.dto;

import java.util.List;

/** Pipeline 主动拉取订单事件命令；eventIds为空时从 afterEventId 后按limit拉取。 */
public record OrderEventPullCommand(List<Long> eventIds, Integer limit, Long afterEventId) {
}
