package com.mtx.trade.pipeline.config;

import com.mtx.trade.pipeline.service.OrderEventStreamConsumer;
import com.mtx.trade.pipeline.service.PaymentEventStreamConsumer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/** 注册订单、支付事件消费及富友报文配置。 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({
        OrderEventConsumerProperties.class,
        PaymentEventConsumerProperties.class,
        ExhaustedEventPullProperties.class,
        FuiouOrderProperties.class,
        FuiouPaymentProperties.class
})
public class EventConsumerConfiguration {

    public static final String ORDER_STREAM_EXECUTOR = "orderStreamListenerExecutor";
    public static final String PAYMENT_STREAM_EXECUTOR = "paymentStreamListenerExecutor";
    public static final String ORDER_PEL_SCHEDULER = "orderPelTaskScheduler";
    public static final String PAYMENT_PEL_SCHEDULER = "paymentPelTaskScheduler";
    public static final String EXHAUSTED_PULL_SCHEDULER = "exhaustedPullTaskScheduler";
    public static final String EXHAUSTED_PULL_WORKER_EXECUTOR = "exhaustedPullWorkerExecutor";

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

    @Bean
    @ConditionalOnProperty(prefix = "trade.pipeline.order-event-consumer",
            name = "enabled", havingValue = "true", matchIfMissing = true)
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> orderStreamListenerContainer(
            RedisConnectionFactory connectionFactory,
            OrderEventConsumerProperties properties,
            OrderEventStreamConsumer consumer,
            @Qualifier(ORDER_STREAM_EXECUTOR) TaskExecutor executor) {
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
                createContainer(connectionFactory, properties.getBlockTimeout(), properties.getBatchSize(),
                        executor, consumer::handleStreamReadError);
        container.register(StreamMessageListenerContainer.StreamReadRequest
                        .builder(StreamOffset.create(properties.getStreamKey(), ReadOffset.lastConsumed()))
                        .consumer(Consumer.from(properties.getGroup(), properties.getConsumerName()))
                        .autoAcknowledge(false)
                        .errorHandler(consumer::handleStreamReadError)
                        .cancelOnError(error -> false)
                        .build(),
                consumer::consumeNewMessage);
        return container;
    }

    @Bean
    @ConditionalOnProperty(prefix = "trade.pipeline.payment-event-consumer",
            name = "enabled", havingValue = "true", matchIfMissing = true)
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> paymentStreamListenerContainer(
            RedisConnectionFactory connectionFactory,
            PaymentEventConsumerProperties properties,
            PaymentEventStreamConsumer consumer,
            @Qualifier(PAYMENT_STREAM_EXECUTOR) TaskExecutor executor) {
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
                createContainer(connectionFactory, properties.getBlockTimeout(), properties.getBatchSize(),
                        executor, consumer::handleStreamReadError);
        container.register(StreamMessageListenerContainer.StreamReadRequest
                        .builder(StreamOffset.create(properties.getStreamKey(), ReadOffset.lastConsumed()))
                        .consumer(Consumer.from(properties.getGroup(), properties.getConsumerName()))
                        .autoAcknowledge(false)
                        .errorHandler(consumer::handleStreamReadError)
                        .cancelOnError(error -> false)
                        .build(),
                consumer::consumeNewMessage);
        return container;
    }

    @Bean(name = ORDER_PEL_SCHEDULER)
    public ThreadPoolTaskScheduler orderPelTaskScheduler() {
        return scheduler(1, "pipeline-order-pel-");
    }

    @Bean(name = PAYMENT_PEL_SCHEDULER)
    public ThreadPoolTaskScheduler paymentPelTaskScheduler() {
        return scheduler(1, "pipeline-payment-pel-");
    }

    @Bean(name = EXHAUSTED_PULL_SCHEDULER)
    public ThreadPoolTaskScheduler exhaustedPullTaskScheduler() {
        return scheduler(2, "pipeline-exhausted-pull-");
    }

    @Bean(name = EXHAUSTED_PULL_WORKER_EXECUTOR)
    public ThreadPoolTaskExecutor exhaustedPullWorkerExecutor(ExhaustedEventPullProperties properties) {
        int parallelism = properties.getParallelism();
        if (parallelism <= 0 || parallelism > 16) {
            throw new IllegalArgumentException("trade.pipeline.exhausted-event-pull.parallelism 必须为1~16");
        }
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(parallelism);
        executor.setMaxPoolSize(parallelism);
        executor.setQueueCapacity(1000);
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("pipeline-exhausted-worker-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        return executor;
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
