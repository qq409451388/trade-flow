package com.mtx.trade.pipeline.exception;

/** 同一 paySsn 并发首次插入时触发，供事务外立即重试。 */
public class ConcurrentPaymentInsertException extends RuntimeException {

    public ConcurrentPaymentInsertException(Throwable cause) {
        super(cause);
    }
}
