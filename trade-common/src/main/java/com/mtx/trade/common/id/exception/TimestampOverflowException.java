package com.mtx.trade.common.id.exception;

/**
 * 时间戳超过 41 位范围时抛出。
 *
 * <p>雪花算法 41 位时间戳在 epoch 后约 69.7 年溢出。</p>
 */
public class TimestampOverflowException extends IdGenerationException {

    private final long currentTimestamp;
    private final long maxTimestamp;

    public TimestampOverflowException(long currentTimestamp, long maxTimestamp) {
        super(String.format(
                "Timestamp overflow: currentTimestamp=%d exceeds maxTimestamp=%d (41-bit range from epoch)",
                currentTimestamp, maxTimestamp));
        this.currentTimestamp = currentTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    public long getCurrentTimestamp() {
        return currentTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }
}
