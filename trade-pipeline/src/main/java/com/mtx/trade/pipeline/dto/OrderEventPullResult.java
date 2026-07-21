package com.mtx.trade.pipeline.dto;

/** Pipeline 主动拉取单个事件的处理结果。 */
public record OrderEventPullResult(Long eventId, String status, String message) {
}
