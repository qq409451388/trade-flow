package com.mtx.trade.ingress.dto;

/** 批量 ACK 的分类统计。 */
public record EventBatchAckResult(int requested, int newlyAcked, int alreadyAcked, int notFound) {
}
