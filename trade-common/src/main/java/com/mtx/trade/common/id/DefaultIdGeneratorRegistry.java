package com.mtx.trade.common.id;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 默认 ID 生成器注册中心实现。
 *
 * <ul>
 *   <li>{@link #global()} 返回委托 {@link SnowflakeIdEngine} 的全局生成器；</li>
 *   <li>{@link #forDomain(String)} 优先返回显式注册的领域生成器，否则动态创建默认领域生成器；</li>
 *   <li>同一领域名称始终返回同一实例（线程安全缓存 {@link ConcurrentHashMap}）。</li>
 * </ul>
 */
public class DefaultIdGeneratorRegistry implements IdGeneratorRegistry {

    private final SnowflakeIdEngine engine;
    private final GlobalIdGenerator globalGenerator;
    private final Map<String, DomainIdGenerator> explicitGenerators;
    private final ConcurrentHashMap<String, DomainIdGenerator> domainCache = new ConcurrentHashMap<>();

    /**
     * @param engine              底层雪花核心
     * @param explicitGenerators  显式注册的领域生成器（key 为标准化后的领域名）
     */
    public DefaultIdGeneratorRegistry(SnowflakeIdEngine engine,
                                      Map<String, DomainIdGenerator> explicitGenerators) {
        if (engine == null) {
            throw new IllegalArgumentException("SnowflakeIdEngine must not be null");
        }
        this.engine = engine;
        this.globalGenerator = new DefaultGlobalIdGenerator(engine);
        this.explicitGenerators = explicitGenerators == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new ConcurrentHashMap<>(explicitGenerators));
    }

    /**
     * 简化构造：无显式注册领域，全部按需动态创建。
     */
    public DefaultIdGeneratorRegistry(SnowflakeIdEngine engine) {
        this(engine, Collections.emptyMap());
    }

    @Override
    public GlobalIdGenerator global() {
        return globalGenerator;
    }

    @Override
    public DomainIdGenerator forDomain(String domain) {
        String normalized = IdGeneratorRegistry.normalize(domain);
        DomainIdGenerator existing = domainCache.get(normalized);
        if (existing != null) {
            return existing;
        }
        DomainIdGenerator explicit = explicitGenerators.get(normalized);
        DomainIdGenerator target = (explicit != null)
                ? explicit
                : new DefaultDomainIdGenerator(normalized, engine);
        DomainIdGenerator previous = domainCache.putIfAbsent(normalized, target);
        return (previous != null) ? previous : target;
    }

    /**
     * 返回已缓存的领域生成器数量（含显式注册与动态创建）。
     */
    public int cachedDomainCount() {
        return domainCache.size();
    }
}
