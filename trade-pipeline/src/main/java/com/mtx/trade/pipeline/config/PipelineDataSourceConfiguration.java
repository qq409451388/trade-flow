package com.mtx.trade.pipeline.config;

import com.baomidou.mybatisplus.autoconfigure.MybatisPlusProperties;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.infra.config.mode.ModeConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** Pipeline 业务库及订单分表数据源配置，与 Storage 数据源完全隔离。 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({
        DataSourceProperties.class,
        MybatisPlusProperties.class,
        PipelineShardingProperties.class
})
@MapperScan(
        basePackages = "com.mtx.trade.pipeline.mapper",
        sqlSessionFactoryRef = "sqlSessionFactory")
public class PipelineDataSourceConfiguration {

    @Bean(name = "pipelineActualDataSource")
    public HikariDataSource pipelineActualDataSource(DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
    }

    @Bean(name = "dataSource")
    @Primary
    public DataSource pipelineDataSource(
            @Qualifier("pipelineActualDataSource") DataSource actualDataSource,
            PipelineShardingProperties properties) throws SQLException {
        Map<String, DataSource> dataSources = new LinkedHashMap<>();
        dataSources.put(PipelineShardingRuleFactory.DATA_SOURCE_NAME, actualDataSource);

        Properties shardingProperties = new Properties();
        shardingProperties.setProperty("sql-show", Boolean.toString(properties.isSqlShow()));
        return ShardingSphereDataSourceFactory.createDataSource(
                "trade_pipeline",
                new ModeConfiguration("Memory", null),
                dataSources,
                Collections.singletonList(pipelineShardingRuleConfiguration(properties)),
                shardingProperties);
    }

    @Bean
    public ShardingRuleConfiguration pipelineShardingRuleConfiguration(PipelineShardingProperties properties) {
        return PipelineShardingRuleFactory.create(properties);
    }

    @Bean(name = "sqlSessionFactory")
    @Primary
    public SqlSessionFactory sqlSessionFactory(
            @Qualifier("dataSource") DataSource dataSource,
            MybatisPlusProperties mybatisPlusProperties) throws Exception {
        MybatisSqlSessionFactoryBean factory = new MybatisSqlSessionFactoryBean();
        factory.setDataSource(dataSource);
        MybatisConfiguration configuration = new MybatisConfiguration();
        mybatisPlusProperties.getConfiguration().applyTo(configuration);
        factory.setConfiguration(configuration);
        factory.setGlobalConfig(mybatisPlusProperties.getGlobalConfig());
        factory.setTypeAliasesPackage("com.mtx.trade.pipeline.entity");
        return factory.getObject();
    }

    @Bean(name = {"transactionManager", "pipelineTransactionManager"})
    @Primary
    public PlatformTransactionManager pipelineTransactionManager(
            @Qualifier("dataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}
