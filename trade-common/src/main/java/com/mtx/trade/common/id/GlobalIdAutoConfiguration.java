package com.mtx.trade.common.id;

import com.mtx.trade.common.id.exception.DuplicateDomainRegistrationException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 全局 ID 生成核心自动配置。
 *
 * <p>注册以下 Bean：
 * <ul>
 *   <li>{@link TimeProvider} — 时间提供者（默认系统时钟）</li>
 *   <li>{@link SnowflakeIdEngine} — 全局雪花核心（单例）</li>
 *   <li>{@link GlobalIdGenerator} — 全局生成器</li>
 *   <li>{@link IdGeneratorRegistry} — 注册中心（收集显式注册的领域生成器）</li>
 * </ul>
 *
 * <p>MyBatis-Plus 适配器见 {@link MyBatisPlusIdAutoConfiguration}。</p>
 */
@AutoConfiguration
@EnableConfigurationProperties(GlobalIdProperties.class)
@ConditionalOnProperty(prefix = "global-id", name = "enabled", havingValue = "true", matchIfMissing = true)
public class GlobalIdAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public TimeProvider timeProvider() {
        return new SystemTimeProvider();
    }

    @Bean
    @ConditionalOnMissingBean
    public SnowflakeIdEngine snowflakeIdEngine(GlobalIdProperties properties, TimeProvider timeProvider) {
        GlobalIdProperties.SnowflakeProperties snow = properties.getSnowflake();
        return new SnowflakeIdEngine(
                snow.getEpoch(),
                snow.getDatacenterId(),
                snow.getWorkerId(),
                snow.getMaxClockBackwardMs(),
                timeProvider
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public GlobalIdGenerator globalIdGenerator(SnowflakeIdEngine engine) {
        return new DefaultGlobalIdGenerator(engine);
    }

    @Bean
    @ConditionalOnMissingBean
    public IdGeneratorRegistry idGeneratorRegistry(
            SnowflakeIdEngine engine,
            TimeProvider timeProvider,
            GlobalIdProperties properties,
            ObjectProvider<DomainIdGenerator> domainGenerators) {

        // 收集显式注册的 DomainIdGenerator Bean，检测重复注册
        Map<String, DomainIdGenerator> explicit = new LinkedHashMap<>();
        for (DomainIdGenerator gen : domainGenerators) {
            String normalized = IdGeneratorRegistry.normalize(gen.domain());
            DomainIdGenerator existing = explicit.get(normalized);
            if (existing != null && existing != gen) {
                throw new DuplicateDomainRegistrationException(normalized);
            }
            explicit.put(normalized, gen);
        }

        Set<String> occupiedNodes = new HashSet<>();
        occupiedNodes.add(nodeKey(engine.getDatacenterId(), engine.getWorkerId()));

        // 配置为 independent 的领域拥有独立引擎；其节点号不得与本进程其他引擎重复。
        if (properties.getDomains() != null) {
            for (Map.Entry<String, GlobalIdProperties.DomainProperties> entry
                    : properties.getDomains().entrySet()) {
                String domain = IdGeneratorRegistry.normalize(entry.getKey());
                GlobalIdProperties.DomainProperties domainProperties = entry.getValue();
                if (domainProperties == null || !domainProperties.isIndependent()) {
                    continue;
                }
                if (explicit.containsKey(domain)) {
                    throw new DuplicateDomainRegistrationException(domain);
                }
                long datacenterId = requiredNodeId(domain, "datacenter-id",
                        domainProperties.getDatacenterId());
                long workerId = requiredNodeId(domain, "worker-id", domainProperties.getWorkerId());
                String nodeKey = nodeKey(datacenterId, workerId);
                if (!occupiedNodes.add(nodeKey)) {
                    throw new IllegalStateException("Duplicate Snowflake node (datacenterId="
                            + datacenterId + ", workerId=" + workerId + ") for domain: " + domain);
                }
                GlobalIdProperties.SnowflakeProperties snow = properties.getSnowflake();
                SnowflakeIdEngine domainEngine = new SnowflakeIdEngine(
                        snow.getEpoch(), datacenterId, workerId,
                        snow.getMaxClockBackwardMs(), timeProvider);
                explicit.put(domain, new DefaultDomainIdGenerator(domain, domainEngine));
            }
        }

        DefaultIdGeneratorRegistry registry = new DefaultIdGeneratorRegistry(engine, explicit);

        // independent=false 的领域也在启动时预注册，但仍共享全局引擎。
        if (properties.getDomains() != null) {
            for (String domain : properties.getDomains().keySet()) {
                registry.forDomain(domain);
            }
        }

        return registry;
    }

    private static long requiredNodeId(String domain, String propertyName, Long value) {
        if (value == null) {
            throw new IllegalStateException("global-id.domains." + domain + "." + propertyName
                    + " is required when independent=true");
        }
        return value;
    }

    private static String nodeKey(long datacenterId, long workerId) {
        return datacenterId + ":" + workerId;
    }
}
