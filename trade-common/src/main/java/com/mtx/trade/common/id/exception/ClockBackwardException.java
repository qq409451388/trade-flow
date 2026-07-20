package com.mtx.trade.common.id.exception;

/**
 * 时钟回拨超出可容忍阈值时抛出。
 *
 * <p>异常中包含：当前时间、上次时间、回拨毫秒数、datacenterId、workerId。</p>
 */
public class ClockBackwardException extends IdGenerationException {

    private final long currentTimestamp;
    private final long lastTimestamp;
    private final long backwardMs;
    private final long datacenterId;
    private final long workerId;

    public ClockBackwardException(long currentTimestamp, long lastTimestamp, long backwardMs,
                                  long datacenterId, long workerId) {
        super(String.format(
                "Clock moved backward by %d ms (exceeds tolerance). currentTimestamp=%d, lastTimestamp=%d, datacenterId=%d, workerId=%d",
                backwardMs, currentTimestamp, lastTimestamp, datacenterId, workerId));
        this.currentTimestamp = currentTimestamp;
        this.lastTimestamp = lastTimestamp;
        this.backwardMs = backwardMs;
        this.datacenterId = datacenterId;
        this.workerId = workerId;
    }

    public long getCurrentTimestamp() {
        return currentTimestamp;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public long getBackwardMs() {
        return backwardMs;
    }

    public long getDatacenterId() {
        return datacenterId;
    }

    public long getWorkerId() {
        return workerId;
    }
}
