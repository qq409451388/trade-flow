package com.mtx.trade.pipeline.config;

import com.mtx.trade.common.utils.EnterpriseWechatRobotUtils;
import com.mtx.trade.common.utils.SpringUtils;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.pipeline.event.consumer.EventStreamListenerRegistry;
import com.mtx.trade.pipeline.event.consumer.OrderEventStreamConsumer;
import com.mtx.trade.pipeline.event.consumer.PartitionedEventExecutor;
import com.mtx.trade.pipeline.event.consumer.PaymentEventStreamConsumer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/** 注册订单、支付事件消费及富友报文配置。 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({
        OrderEventConsumerProperties.class,
        PaymentEventConsumerProperties.class,
        UnackedEventPullProperties.class,
        PerformanceMonitoringProperties.class,
        FuiouOrderProperties.class,
        FuiouPaymentProperties.class
})
public class EventConsumerConfiguration {

    public static final String ORDER_STREAM_EXECUTOR = "orderStreamListenerExecutor";
    public static final String PAYMENT_STREAM_EXECUTOR = "paymentStreamListenerExecutor";
    public static final String ORDER_EVENT_WORKER_EXECUTOR = "orderEventWorkerExecutor";
    public static final String PAYMENT_EVENT_WORKER_EXECUTOR = "paymentEventWorkerExecutor";
    public static final String STREAM_WATCHDOG_SCHEDULER = "streamWatchdogTaskScheduler";
    public static final String ORDER_PEL_SCHEDULER = "orderPelTaskScheduler";
    public static final String PAYMENT_PEL_SCHEDULER = "paymentPelTaskScheduler";
    public static final String UNACKED_PULL_SCHEDULER = "unackedPullTaskScheduler";
    public static final String UNACKED_PULL_WORKER_EXECUTOR = "unackedPullWorkerExecutor";
    public static final String PERFORMANCE_MONITOR_SCHEDULER = "performanceMonitorTaskScheduler";

    @Bean(name = ORDER_STREAM_EXECUTOR)
    @ConditionalOnProperty(prefix = "trade.pipeline.order-event-consumer",
            name = "enabled", havingValue = "true", matchIfMissing = true)
    public ThreadPoolTaskExecutor orderStreamListenerExecutor() {
        return streamExecutor("pipeline-order-stream-");
    }

    @Bean(name = PAYMENT_STREAM_EXECUTOR)
    @ConditionalOnProperty(prefix = "trade.pipeline.payment-event-consumer",
            name = "enabled", havingValue = "true", matchIfMissing = true)
    public ThreadPoolTaskExecutor paymentStreamListenerExecutor() {
        return streamExecutor("pipeline-payment-stream-");
    }

    @Bean(name = ORDER_EVENT_WORKER_EXECUTOR, destroyMethod = "shutdown")
    @ConditionalOnProperty(prefix = "trade.pipeline.order-event-consumer",
            name = "enabled", havingValue = "true", matchIfMissing = true)
    public PartitionedEventExecutor orderEventWorkerExecutor(OrderEventConsumerProperties properties) {
        return new PartitionedEventExecutor(
                properties.getWorkerCount(),
                properties.getWorkerQueueCapacity(),
                "pipeline-order-worker-");
    }

    @Bean(name = PAYMENT_EVENT_WORKER_EXECUTOR, destroyMethod = "shutdown")
    @ConditionalOnProperty(prefix = "trade.pipeline.payment-event-consumer",
            name = "enabled", havingValue = "true", matchIfMissing = true)
    public PartitionedEventExecutor paymentEventWorkerExecutor(PaymentEventConsumerProperties properties) {
        return new PartitionedEventExecutor(
                properties.getWorkerCount(),
                properties.getWorkerQueueCapacity(),
                "pipeline-payment-worker-");
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnProperty(prefix = "trade.pipeline.order-event-consumer",
            name = "enabled", havingValue = "true", matchIfMissing = true)
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> orderStreamListenerContainer(
            RedisConnectionFactory connectionFactory,
            OrderEventConsumerProperties properties,
            OrderEventStreamConsumer consumer,
            EventStreamListenerRegistry listenerRegistry,
            @Qualifier(ORDER_STREAM_EXECUTOR) TaskExecutor executor) {
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
                createContainer(connectionFactory, properties.getBlockTimeout(), properties.getBatchSize(),
                        executor, consumer::handleStreamReadError);
        var request = StreamMessageListenerContainer.StreamReadRequest
                        .builder(StreamOffset.create(properties.getStreamKey(), ReadOffset.lastConsumed()))
                        .consumer(Consumer.from(properties.getGroup(), properties.getConsumerName()))
                        .autoAcknowledge(false)
                        .errorHandler(consumer::handleStreamReadError)
                        .cancelOnError(error -> false)
                        .build();
        Subscription subscription = container.register(request, consumer::consumeNewMessage);
        listenerRegistry.bind(ContentType.ORDER.getCode(), "order", container, subscription,
                () -> container.register(request, consumer::consumeNewMessage));
        return container;
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnProperty(prefix = "trade.pipeline.payment-event-consumer",
            name = "enabled", havingValue = "true", matchIfMissing = true)
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> paymentStreamListenerContainer(
            RedisConnectionFactory connectionFactory,
            PaymentEventConsumerProperties properties,
            PaymentEventStreamConsumer consumer,
            EventStreamListenerRegistry listenerRegistry,
            @Qualifier(PAYMENT_STREAM_EXECUTOR) TaskExecutor executor) {
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
                createContainer(connectionFactory, properties.getBlockTimeout(), properties.getBatchSize(),
                        executor, consumer::handleStreamReadError);
        var request = StreamMessageListenerContainer.StreamReadRequest
                        .builder(StreamOffset.create(properties.getStreamKey(), ReadOffset.lastConsumed()))
                        .consumer(Consumer.from(properties.getGroup(), properties.getConsumerName()))
                        .autoAcknowledge(false)
                        .errorHandler(consumer::handleStreamReadError)
                        .cancelOnError(error -> false)
                        .build();
        Subscription subscription = container.register(request, consumer::consumeNewMessage);
        listenerRegistry.bind(ContentType.PAYMENT.getCode(), "payment", container, subscription,
                () -> container.register(request, consumer::consumeNewMessage));
        return container;
    }

    @Bean(name = STREAM_WATCHDOG_SCHEDULER)
    public ThreadPoolTaskScheduler streamWatchdogTaskScheduler() {
        return scheduler(2, "pipeline-stream-watchdog-");
    }

    @Bean(name = ORDER_PEL_SCHEDULER)
    public ThreadPoolTaskScheduler orderPelTaskScheduler() {
        return scheduler(1, "pipeline-order-pel-");
    }

    @Bean(name = PAYMENT_PEL_SCHEDULER)
    public ThreadPoolTaskScheduler paymentPelTaskScheduler() {
        return scheduler(1, "pipeline-payment-pel-");
    }

    @Bean(name = UNACKED_PULL_SCHEDULER)
    public ThreadPoolTaskScheduler unackedPullTaskScheduler() {
        return scheduler(2, "pipeline-unacked-pull-");
    }

    @Bean(name = PERFORMANCE_MONITOR_SCHEDULER)
    public ThreadPoolTaskScheduler performanceMonitorTaskScheduler() {
        return scheduler(1, "pipeline-performance-monitor-");
    }

    @Bean(name = UNACKED_PULL_WORKER_EXECUTOR)
    public ThreadPoolTaskExecutor unackedPullWorkerExecutor(UnackedEventPullProperties properties) {
        int parallelism = properties.getParallelism();
        if (parallelism <= 0 || parallelism > 16) {
            throw new IllegalArgumentException("trade.pipeline.unacked-event-pull.parallelism 必须为1~16");
        }
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(parallelism);
        executor.setMaxPoolSize(parallelism);
        executor.setQueueCapacity(1000);
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("pipeline-unacked-worker-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        return executor;
    }

    @Bean
    public EnterpriseWechatRobotUtils enterpriseWechatRobotUtils(Environment environment) {
        return new EnterpriseWechatRobotUtils(new SpringUtils(environment));
    }

    private static StreamMessageListenerContainer<String, MapRecord<String, String, String>> createContainer(
            RedisConnectionFactory connectionFactory,
            java.time.Duration pollTimeout,
            int batchSize,
            TaskExecutor executor,
            org.springframework.util.ErrorHandler errorHandler) {
        var options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
                .pollTimeout(pollTimeout)
                .batchSize(batchSize)
                .executor(executor)
                .serializer(RedisSerializer.string())
                .errorHandler(errorHandler)
                .build();
        return StreamMessageListenerContainer.create(connectionFactory, options);
    }

    private static ThreadPoolTaskExecutor streamExecutor(String threadNamePrefix) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix(threadNamePrefix);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(10);
        return executor;
    }

    private static ThreadPoolTaskScheduler scheduler(int poolSize, String threadNamePrefix) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(poolSize);
        scheduler.setThreadNamePrefix(threadNamePrefix);
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(10);
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }
}
