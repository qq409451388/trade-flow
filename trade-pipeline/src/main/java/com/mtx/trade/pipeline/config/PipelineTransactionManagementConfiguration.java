package com.mtx.trade.pipeline.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.TransactionManagementConfigurer;

/**
 * Pipeline 应用的默认声明式事务配置。
 *
 * <p>业务代码和 MyBatis-Plus 未命名事务统一使用 Pipeline 事务管理器；Storage 跨库操作继续通过
 * {@code storageTransactionManager} 显式绑定，禁止依赖 {@code @Primary} 猜测。</p>
 */
@Configuration(proxyBeanMethods = false)
@EnableTransactionManagement
public class PipelineTransactionManagementConfiguration implements TransactionManagementConfigurer {

    private final TransactionManager pipelineTransactionManager;

    public PipelineTransactionManagementConfiguration(
            @Qualifier("pipelineTransactionManager") TransactionManager pipelineTransactionManager) {
        this.pipelineTransactionManager = pipelineTransactionManager;
    }

    @Override
    public TransactionManager annotationDrivenTransactionManager() {
        return pipelineTransactionManager;
    }
}
