package com.mtx.trade.ingress.dto;

import java.util.function.Predicate;

/**
 * Event 入库结果。
 *
 * @param event 当前有效的 event
 * @param accepted 本次消息是否新增了 event；false 时 event 为已存在的同业务版本记录
 */
public record EventIngestResult<T>(T event, boolean accepted) {

    /** 新事件需要发布；重复事件仅在尚未 ACK 时重新发布，以利用上游重推缩短恢复延迟。 */
    public boolean shouldPublish(Predicate<T> unackedPredicate) {
        return event != null && (accepted || unackedPredicate.test(event));
    }
}
