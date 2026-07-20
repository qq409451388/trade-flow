package com.mtx.trade.common.id;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import com.baomidou.mybatisplus.core.incrementer.IdentifierGenerator;

/**
 * MyBatis-Plus IdentifierGenerator 适配器自动配置。
 *
 * <p>仅在 classpath 存在 MyBatis-Plus 且项目未自定义 {@link IdentifierGenerator} 时激活。
 * 通过 {@code @AutoConfiguration(beforeName = ...)} 确保在 MyBatis-Plus 的
 * {@code IdentifierGeneratorAutoConfiguration} 之前注册，
 * 使 MyBatis-Plus 的 {@code @ConditionalOnMissingBean} 跳过其默认实现。</p>
 */
@AutoConfiguration(beforeName = "com.baomidou.mybatisplus.autoconfigure.IdentifierGeneratorAutoConfiguration")
@ConditionalOnClass(IdentifierGenerator.class)
@ConditionalOnProperty(prefix = "global-id", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MyBatisPlusIdAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(IdentifierGenerator.class)
    public SnowflakeIdentifierGenerator snowflakeIdentifierGenerator(GlobalIdGenerator globalIdGenerator) {
        return new SnowflakeIdentifierGenerator(globalIdGenerator);
    }
}
