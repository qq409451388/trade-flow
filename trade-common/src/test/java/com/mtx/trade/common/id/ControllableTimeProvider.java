package com.mtx.trade.common.id;

/**
 * 测试用可控时间提供者。
 * 可手动设置或推进时间，模拟时钟回拨、同一毫秒内序列耗尽等场景。
 */
class ControllableTimeProvider implements TimeProvider {

    private volatile long current;

    ControllableTimeProvider(long start) {
        this.current = start;
    }

    void set(long t) {
        this.current = t;
    }

    void advance(long ms) {
        this.current += ms;
    }

    @Override
    public long currentTimeMillis() {
        return current;
    }
}
