package com.mtx.trade.ingress.dto;

/**
 * Event 入库结果。
 *
 * @param event 当前有效的 event
 * @param accepted 本次消息是否新增或推进了 event 版本；false 时不应再次发布给消费端
 */
public record EventIngestResult<T>(T event, boolean accepted) {
}
