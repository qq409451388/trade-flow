package com.mtx.trade.pipeline.service;

/** 订单事件持久化结果。 */
public enum OrderPersistResult {
    APPLIED,
    IGNORED_DUPLICATE_OR_STALE
}
