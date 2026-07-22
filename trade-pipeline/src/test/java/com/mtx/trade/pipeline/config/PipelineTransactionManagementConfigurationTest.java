package com.mtx.trade.pipeline.config;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.SimpleTransactionStatus;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineTransactionManagementConfigurationTest {

    @Test
    void shouldUsePipelineManagerForUnqualifiedTransactionalMethods() {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(TestConfiguration.class)) {
            context.getBean(UnqualifiedTransactionalService.class).execute();

            TrackingTransactionManager pipelineManager =
                    context.getBean("pipelineTransactionManager", TrackingTransactionManager.class);
            TrackingTransactionManager storageManager =
                    context.getBean("storageTransactionManager", TrackingTransactionManager.class);
            assertThat(pipelineManager.started).isEqualTo(1);
            assertThat(pipelineManager.committed).isEqualTo(1);
            assertThat(storageManager.started).isZero();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(PipelineTransactionManagementConfiguration.class)
    static class TestConfiguration {

        @Bean("pipelineTransactionManager")
        TrackingTransactionManager pipelineTransactionManager() {
            return new TrackingTransactionManager();
        }

        @Bean("storageTransactionManager")
        TrackingTransactionManager storageTransactionManager() {
            return new TrackingTransactionManager();
        }

        @Bean
        UnqualifiedTransactionalService unqualifiedTransactionalService() {
            return new UnqualifiedTransactionalService();
        }
    }

    static class UnqualifiedTransactionalService {

        @Transactional
        public void execute() {
        }
    }

    static class TrackingTransactionManager implements PlatformTransactionManager {

        private int started;
        private int committed;

        @Override
        public TransactionStatus getTransaction(TransactionDefinition definition) {
            started++;
            return new SimpleTransactionStatus();
        }

        @Override
        public void commit(TransactionStatus status) {
            committed++;
        }

        @Override
        public void rollback(TransactionStatus status) {
        }
    }
}
