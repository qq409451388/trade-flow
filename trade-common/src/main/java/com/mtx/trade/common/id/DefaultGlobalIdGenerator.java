package com.mtx.trade.common.id;

/**
 * 默认全局 ID 生成器实现，委托底层 {@link SnowflakeIdEngine}。
 */
public class DefaultGlobalIdGenerator implements GlobalIdGenerator {

    private final SnowflakeIdEngine engine;

    public DefaultGlobalIdGenerator(SnowflakeIdEngine engine) {
        if (engine == null) {
            throw new IllegalArgumentException("SnowflakeIdEngine must not be null");
        }
        this.engine = engine;
    }

    @Override
    public long nextId() {
        return engine.nextId();
    }
}
