package com.mtx.trade.pipeline.service;

/** 订单首次创建时唯一键并发竞争；外层应在原事务回滚后立即重试一次。 */
public class ConcurrentOrderInsertException extends RuntimeException {

    public ConcurrentOrderInsertException(Throwable cause) {
        super(cause);
    }
}
