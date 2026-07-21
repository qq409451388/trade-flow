package com.mtx.trade.common.id;

/**
 * 默认领域 ID 生成器实现，委托底层 {@link SnowflakeIdEngine}。
 *
 * <p>领域名称不编码到 ID 中。生成器可以委托全局引擎，也可以委托配置的独立领域引擎；
 * 跨领域唯一性依赖各独立引擎使用不同的节点组合。</p>
 */
public class DefaultDomainIdGenerator implements DomainIdGenerator {

    private final String domain;
    private final SnowflakeIdEngine engine;

    /**
     * @param domain 领域名称（自动标准化：去首尾空格、转小写）
     * @param engine 底层雪花核心
     */
    public DefaultDomainIdGenerator(String domain, SnowflakeIdEngine engine) {
        if (engine == null) {
            throw new IllegalArgumentException("SnowflakeIdEngine must not be null");
        }
        this.domain = IdGeneratorRegistry.normalize(domain);
        this.engine = engine;
    }

    @Override
    public long nextId() {
        return engine.nextId();
    }

    @Override
    public String domain() {
        return domain;
    }
}
