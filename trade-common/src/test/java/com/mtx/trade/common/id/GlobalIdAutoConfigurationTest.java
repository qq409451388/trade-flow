package com.mtx.trade.common.id;

import com.baomidou.mybatisplus.core.incrementer.IdentifierGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spring Boot 自动配置测试。
 *
 * <p>验证自动配置正确注册了所有核心 Bean 和 MyBatis-Plus 适配器。</p>
 */
class GlobalIdAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    GlobalIdAutoConfiguration.class,
                    MyBatisPlusIdAutoConfiguration.class));

    @Test
    @DisplayName("默认配置注册核心 Bean")
    void registersCoreBeans() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(SnowflakeIdEngine.class);
            assertThat(context).hasSingleBean(GlobalIdGenerator.class);
            assertThat(context).hasSingleBean(IdGeneratorRegistry.class);
            assertThat(context).hasSingleBean(TimeProvider.class);
            assertThat(context).hasSingleBean(SystemTimeProvider.class);
        });
    }

    @Test
    @DisplayName("注册 MyBatis-Plus IdentifierGenerator 适配器")
    void registersMyBatisPlusAdapter() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(SnowflakeIdentifierGenerator.class);
            assertThat(context.getBean(SnowflakeIdentifierGenerator.class))
                    .isInstanceOf(IdentifierGenerator.class);
        });
    }

    @Test
    @DisplayName("配置自定义 datacenterId 和 workerId")
    void customDatacenterAndWorker() {
        runner.withPropertyValues(
                "global-id.snowflake.datacenter-id=5",
                "global-id.snowflake.worker-id=10",
                "global-id.snowflake.max-clock-backward-ms=20"
        ).run(context -> {
            SnowflakeIdEngine engine = context.getBean(SnowflakeIdEngine.class);
            assertThat(engine.getDatacenterId()).isEqualTo(5);
            assertThat(engine.getWorkerId()).isEqualTo(10);
            assertThat(engine.getMaxClockBackwardMs()).isEqualTo(20);
        });
    }

    @Test
    @DisplayName("配置预注册领域")
    void preConfiguredDomains() {
        runner.withPropertyValues(
                "global-id.domains[0]=order",
                "global-id.domains[1]=payment",
                "global-id.domains[2]=event"
        ).run(context -> {
            IdGeneratorRegistry registry = context.getBean(IdGeneratorRegistry.class);
            DomainIdGenerator order = registry.forDomain("order");
            DomainIdGenerator payment = registry.forDomain("payment");
            DomainIdGenerator event = registry.forDomain("event");

            assertThat(order.domain()).isEqualTo("order");
            assertThat(payment.domain()).isEqualTo("payment");
            assertThat(event.domain()).isEqualTo("event");

            // 验证能正常生成 ID
            assertThat(order.nextId()).isPositive();
            assertThat(payment.nextId()).isPositive();
            assertThat(event.nextId()).isPositive();
        });
    }

    @Test
    @DisplayName("global-id.enabled=false 时不注册 Bean")
    void disabledByProperty() {
        runner.withPropertyValues("global-id.enabled=false").run(context -> {
            assertThat(context).doesNotHaveBean(SnowflakeIdEngine.class);
            assertThat(context).doesNotHaveBean(GlobalIdGenerator.class);
            assertThat(context).doesNotHaveBean(IdGeneratorRegistry.class);
        });
    }

    @Test
    @DisplayName("datacenterId 越界时启动失败")
    void invalidDatacenterIdFails() {
        runner.withPropertyValues("global-id.snowflake.datacenter-id=32")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .getRootCause()
                            .isInstanceOf(com.mtx.trade.common.id.exception.InvalidDatacenterIdException.class);
                });
    }

    @Test
    @DisplayName("workerId 越界时启动失败")
    void invalidWorkerIdFails() {
        runner.withPropertyValues("global-id.snowflake.worker-id=99")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .getRootCause()
                            .isInstanceOf(com.mtx.trade.common.id.exception.InvalidWorkerIdException.class);
                });
    }

    @Test
    @DisplayName("全局生成器可生成正数 ID")
    void globalGeneratorProducesPositiveId() {
        runner.run(context -> {
            GlobalIdGenerator generator = context.getBean(GlobalIdGenerator.class);
            long id = generator.nextId();
            assertThat(id).isPositive();
        });
    }
}
