package com.mtx.trade.common.id;

/**
 * 基于系统时钟的时间提供者，生产环境默认使用。
 */
public class SystemTimeProvider implements TimeProvider {

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
