package com.mtx.trade.storage.local.config;

import com.baomidou.mybatisplus.autoconfigure.MybatisPlusAutoConfiguration;
import com.baomidou.mybatisplus.autoconfigure.MybatisPlusProperties;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.mtx.trade.storage.api.StorageReader;
import com.mtx.trade.storage.api.StorageIdGenerator;
import com.mtx.trade.storage.api.StorageWriter;
import com.mtx.trade.storage.local.LocalStorageAdapter;
import com.mtx.trade.storage.local.LocalStorageReaderAdapter;
import com.mtx.trade.storage.local.service.db.StorageBlobDbService;
import com.mtx.trade.storage.local.service.db.StorageDbService;
import com.mtx.trade.storage.local.service.db.impl.StorageBlobDbServiceImpl;
import com.mtx.trade.storage.local.service.db.impl.StorageDbServiceImpl;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.infra.config.mode.ModeConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Storage 本地 MySQL adapter 自动配置。
 *
 * <p>仅当显式配置 {@code trade.storage.local.enabled=true} 时启用。所有 bean 均使用 storage 前缀命名，
 * 不抢占应用的 {@code dataSource}/{@code sqlSessionFactory}。</p>
 */
@AutoConfiguration(after = {DataSourceAutoConfiguration.class, MybatisPlusAutoConfiguration.class})
@ConditionalOnClass({ShardingSphereDataSourceFactory.class, MybatisSqlSessionFactoryBean.class})
@ConditionalOnProperty(prefix = "trade.storage.local", name = "enabled", havingValue = "true")
@EnableConfigurationProperties({StorageLocalProperties.class, StorageDataSourceProperties.class})
@MapperScan(
        basePackages = "com.mtx.trade.storage.local.mapper",
        sqlSessionFactoryRef = "storageSqlSessionFactory")
public class StorageLocalAutoConfiguration {

    @Bean(name = "storageActualDataSource")
    @ConfigurationProperties("trade.storage.datasource.hikari")
    public HikariDataSource storageActualDataSource(StorageDataSourceProperties properties) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(properties.getUrl());
        dataSource.setUsername(properties.getUsername());
        dataSource.setPassword(properties.getPassword());
        dataSource.setDriverClassName(properties.getDriverClassName());
        return dataSource;
    }

    @Bean(name = "storageDataSource")
    public DataSource storageDataSource(
            @Qualifier("storageActualDataSource") DataSource actualDataSource,
            StorageLocalProperties properties) throws SQLException {
        Map<String, DataSource> dataSources = new LinkedHashMap<>();
        dataSources.put("storage_mysql", actualDataSource);
        Properties shardingProperties = new Properties();
        shardingProperties.setProperty("sql-show", Boolean.toString(properties.isSqlShow()));
        return ShardingSphereDataSourceFactory.createDataSource(
                "trade_storage",
                new ModeConfiguration("Memory", null),
                dataSources,
                Collections.singletonList(storageShardingRuleConfiguration()),
                shardingProperties);
    }

    @Bean
    public ShardingRuleConfiguration storageShardingRuleConfiguration() {
        return StorageShardingRuleFactory.create();
    }

    @Bean(name = "storageSqlSessionFactory")
    public SqlSessionFactory storageSqlSessionFactory(
            @Qualifier("storageDataSource") DataSource storageDataSource,
            MybatisPlusProperties mybatisPlusProperties) throws Exception {
        MybatisSqlSessionFactoryBean factory = new MybatisSqlSessionFactoryBean();
        factory.setDataSource(storageDataSource);
        MybatisConfiguration configuration = new MybatisConfiguration();
        mybatisPlusProperties.getConfiguration().applyTo(configuration);
        factory.setConfiguration(configuration);
        factory.setGlobalConfig(mybatisPlusProperties.getGlobalConfig());
        factory.setTypeAliasesPackage("com.mtx.trade.storage.local.entity");
        return factory.getObject();
    }

    @Bean(name = "storageTransactionManager")
    public PlatformTransactionManager storageTransactionManager(
            @Qualifier("storageDataSource") DataSource storageDataSource) {
        return new DataSourceTransactionManager(storageDataSource);
    }

    @Bean
    public StorageDbService storageDbService() {
        return new StorageDbServiceImpl();
    }

    @Bean
    public StorageBlobDbService storageBlobDbService() {
        return new StorageBlobDbServiceImpl();
    }

    @Bean
    @ConditionalOnMissingBean(StorageReader.class)
    public LocalStorageReaderAdapter localStorageReaderAdapter(
            StorageDbService storageDbService,
            StorageBlobDbService storageBlobDbService) {
        return new LocalStorageReaderAdapter(storageDbService, storageBlobDbService);
    }

    @Bean
    @ConditionalOnBean(StorageIdGenerator.class)
    @ConditionalOnMissingBean(StorageWriter.class)
    public LocalStorageAdapter localStorageAdapter(
            StorageDbService storageDbService,
            StorageBlobDbService storageBlobDbService,
            StorageIdGenerator storageIdGenerator) {
        return new LocalStorageAdapter(storageDbService, storageBlobDbService, storageIdGenerator);
    }
}
