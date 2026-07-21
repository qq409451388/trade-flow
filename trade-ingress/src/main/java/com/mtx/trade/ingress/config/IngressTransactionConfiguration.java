package com.mtx.trade.ingress.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/** Ingress 业务库事务配置，与 Storage 独立事务管理器隔离。 */
@Configuration(proxyBeanMethods = false)
public class IngressTransactionConfiguration {

    @Bean(name = {"transactionManager", "ingressTransactionManager"})
    @Primary
    public PlatformTransactionManager ingressTransactionManager(
            @Qualifier("dataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}
