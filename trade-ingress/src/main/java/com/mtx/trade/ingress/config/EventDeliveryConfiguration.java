package com.mtx.trade.ingress.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/** Ingress 事件投递调度配置。 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties({
        EventDeliveryProperties.class,
        EventDeliveryCircuitProperties.class,
        StorageWriteProperties.class,
        EventStreamProperties.class
})
public class EventDeliveryConfiguration {

    public static final String EVENT_RETRY_SCHEDULER = "eventRetryTaskScheduler";
    public static final String EVENT_REDELIVERY_SCHEDULER = "eventRedeliveryTaskScheduler";
    public static final String CIRCUIT_RECOVERY_SCHEDULER = "circuitRecoveryTaskScheduler";

    @Bean(name = EVENT_RETRY_SCHEDULER)
    public ThreadPoolTaskScheduler eventRetryTaskScheduler() {
        return scheduler(2, "event-retry-");
    }

    @Bean(name = EVENT_REDELIVERY_SCHEDULER)
    public ThreadPoolTaskScheduler eventRedeliveryTaskScheduler() {
        return scheduler(1, "event-redelivery-");
    }

    @Bean(name = CIRCUIT_RECOVERY_SCHEDULER)
    public ThreadPoolTaskScheduler circuitRecoveryTaskScheduler() {
        return scheduler(1, "circuit-recovery-");
    }

    private static ThreadPoolTaskScheduler scheduler(int poolSize, String threadNamePrefix) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(poolSize);
        scheduler.setThreadNamePrefix(threadNamePrefix);
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setWaitForTasksToCompleteOnShutdown(false);
        return scheduler;
    }
}
