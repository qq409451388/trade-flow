package com.mtx.trade.common.id.exception;

/**
 * datacenterId 越界（不在 0~31 范围内）时抛出，导致启动失败。
 */
public class InvalidDatacenterIdException extends IdGenerationException {

    private final long provided;
    private final long maxAllowed;

    public InvalidDatacenterIdException(long provided, long maxAllowed) {
        super(String.format(
                "Invalid datacenterId=%d, must be in range [0, %d]",
                provided, maxAllowed));
        this.provided = provided;
        this.maxAllowed = maxAllowed;
    }

    public long getProvided() {
        return provided;
    }

    public long getMaxAllowed() {
        return maxAllowed;
    }
}
