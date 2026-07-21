package com.mtx.trade.pipeline.dto;

/** 单条主动拉取支付事件的处理结果。 */
public record PaymentEventPullResult(Long eventId, String status, String message) {
}
