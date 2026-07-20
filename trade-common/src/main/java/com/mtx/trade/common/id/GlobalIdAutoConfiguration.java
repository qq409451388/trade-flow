package com.mtx.trade.common.id;

import com.mtx.trade.common.id.exception.DuplicateDomainRegistrationException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 全局 ID 生成核心自动配置。
 *
 * <p>注册以下 Bean：
 * <ul>
 *   <li>{@link TimeProvider} — 时间提供者（默认系统时钟）</li>
 *   <li>{@link SnowflakeIdEngine} — 雪花核心（单例）</li>
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

        DefaultIdGeneratorRegistry registry = new DefaultIdGeneratorRegistry(engine, explicit);

        // 预注册配置中声明的领域
        if (properties.getDomains() != null) {
            for (String domain : properties.getDomains()) {
                registry.forDomain(domain);
            }
        }

        return registry;
    }
}
