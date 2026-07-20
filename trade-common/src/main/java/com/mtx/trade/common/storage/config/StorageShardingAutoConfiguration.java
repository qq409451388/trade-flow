package com.mtx.trade.common.storage.config;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.config.mode.ModeConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableReferenceRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 存储分表自动配置。
 *
 * <p>当接入方 classpath 存在 ShardingSphere-JDBC 时自动激活，将
 * {@code trade_storage} / {@code trade_storage_blob} 两张逻辑表按
 * {@code id % 100} 分片到 100 张物理分表（两位零填充后缀 _00 ~ _99）。</p>
 *
 * <p>分片规则在此定义，是 {@code StorageDO} / {@code StorageBlobDO} 契约的一部分，
 * 接入方只需使用 Spring Boot 标准的 {@code spring.datasource.*} 配置数据源。</p>
 *
 * <p>通过 {@code @AutoConfiguration(before = DataSourceAutoConfiguration.class)}
 * 抢先注册 {@code ShardingSphereDataSource}，使 Spring Boot 默认数据源自动配置跳过，
 * MyBatis-Plus 等组件直接使用分表数据源。</p>
 */
@AutoConfiguration(before = DataSourceAutoConfiguration.class)
@EnableConfigurationProperties(DataSourceProperties.class)
@ConditionalOnClass(ShardingSphereDataSourceFactory.class)
@ConditionalOnProperty(prefix = "trade.storage.sharding", name = "enabled", havingValue = "true", matchIfMissing = true)
public class StorageShardingAutoConfiguration {

    /** 逻辑数据源名称 */
    private static final String DS_NAME = "ds_0";
    /** 分表数量，固定 100，不可覆盖 */
    private static final int SHARD_COUNT = 100;
    /** 逻辑库名 */
    private static final String DATABASE_NAME = "trade_storage";
    /** 分片列 */
    private static final String SHARD_COLUMN = "id";

    /** 需要分表的逻辑表 */
    private static final String[] SHARD_TABLES = {"trade_storage", "trade_storage_blob"};

    /**
     * 创建由 ShardingSphere 包装的主数据源。
     *
     * <p>{@code storageActualDataSource} 只表示业务方配置的真实连接池；对 MyBatis、事务管理器等
     * 下游组件暴露的始终是名为 {@code dataSource} 的分表数据源。</p>
     */
    @Bean(name = "dataSource")
    @Primary
    @ConditionalOnMissingBean(name = "dataSource")
    public DataSource storageShardingDataSource(
            @Qualifier("storageActualDataSource") DataSource actualDataSource,
            ShardingRuleConfiguration storageShardingRuleConfiguration) throws SQLException {
        Map<String, DataSource> dataSourceMap = new LinkedHashMap<>();
        dataSourceMap.put(DS_NAME, actualDataSource);

        Properties shardingProps = new Properties();
        shardingProps.setProperty("sql-show", "false");

        ModeConfiguration modeConfig = new ModeConfiguration("Memory", null);
        return ShardingSphereDataSourceFactory.createDataSource(
                DATABASE_NAME,
                modeConfig,
                dataSourceMap,
                Collections.singletonList(storageShardingRuleConfiguration),
                shardingProps);
    }

    /**
     * 使用 Spring Boot 标准 {@code spring.datasource} 与
     * {@code spring.datasource.hikari} 配置创建真实连接池。
     */
    @Bean(name = "storageActualDataSource")
    @ConfigurationProperties("spring.datasource.hikari")
    @ConditionalOnMissingBean(name = "storageActualDataSource")
    public HikariDataSource storageActualDataSource(DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    /**
     * storage 表的固定分表契约。规则以独立 Bean 暴露，便于验证和复用，但接入方无需配置。
     */
    @Bean
    public ShardingRuleConfiguration storageShardingRuleConfiguration() {
        ShardingRuleConfiguration ruleConfig = new ShardingRuleConfiguration();
        for (String logicTable : SHARD_TABLES) {
            ruleConfig.getTables().add(buildTableRule(logicTable));
            Properties algoProps = new Properties();
            algoProps.setProperty(
                    "algorithm-expression",
                    logicTable + "_${String.format('%02d', " + SHARD_COLUMN + " % " + SHARD_COUNT + ")}");
            ruleConfig.getShardingAlgorithms().put(logicTable + "_inline", new AlgorithmConfiguration("INLINE", algoProps));
        }
        ruleConfig.getBindingTableGroups().add(
                new ShardingTableReferenceRuleConfiguration(
                        "storage_binding", "trade_storage,trade_storage_blob"));
        return ruleConfig;
    }

    /**
     * 构造单张逻辑表的分片规则：actualDataNodes 手动枚举 100 张物理分表，
     * 避免行表达式零填充的兼容性问题。
     */
    private ShardingTableRuleConfiguration buildTableRule(String logicTable) {
        StringBuilder actualDataNodes = new StringBuilder();
        for (int i = 0; i < SHARD_COUNT; i++) {
            if (i > 0) {
                actualDataNodes.append(",");
            }
            actualDataNodes.append(DS_NAME).append(".").append(logicTable).append("_").append(String.format("%02d", i));
        }
        ShardingTableRuleConfiguration rule = new ShardingTableRuleConfiguration(logicTable, actualDataNodes.toString());
        rule.setTableShardingStrategy(new StandardShardingStrategyConfiguration(SHARD_COLUMN, logicTable + "_inline"));
        return rule;
    }
}
