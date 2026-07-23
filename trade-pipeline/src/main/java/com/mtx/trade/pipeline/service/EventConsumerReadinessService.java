package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.enums.ContentType;

import com.mtx.trade.pipeline.dto.EventConsumerReadinessVO;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;

/** 检查订单consumer真正需要的Pipeline DB、Storage DB、Redis和消费组状态。 */
@Service
public class EventConsumerReadinessService {

    private final DataSource pipelineDataSource;
    private final ObjectProvider<DataSource> storageDataSourceProvider;
    private final StringRedisTemplate redisTemplate;
    private final ObjectProvider<OrderEventStreamConsumer> orderConsumerProvider;
    private final ObjectProvider<PaymentEventStreamConsumer> paymentConsumerProvider;
    private final EventStreamListenerRegistry listenerRegistry;

    public EventConsumerReadinessService(
            @Qualifier("pipelineActualDataSource") DataSource pipelineDataSource,
            @Qualifier("storageActualDataSource") ObjectProvider<DataSource> storageDataSourceProvider,
            StringRedisTemplate redisTemplate,
            ObjectProvider<OrderEventStreamConsumer> orderConsumerProvider,
            ObjectProvider<PaymentEventStreamConsumer> paymentConsumerProvider,
            EventStreamListenerRegistry listenerRegistry) {
        this.pipelineDataSource = pipelineDataSource;
        this.storageDataSourceProvider = storageDataSourceProvider;
        this.redisTemplate = redisTemplate;
        this.orderConsumerProvider = orderConsumerProvider;
        this.paymentConsumerProvider = paymentConsumerProvider;
        this.listenerRegistry = listenerRegistry;
    }

    public EventConsumerReadinessVO check(Integer contentType) {
        Map<String, Boolean> checks = new LinkedHashMap<>();
        boolean order = contentType != null && contentType == ContentType.ORDER.getCode();
        boolean payment = contentType != null && contentType == ContentType.PAYMENT.getCode();
        checks.put("contentTypeSupported", order || payment);
        checks.put("pipelineDatabase", databaseReady(pipelineDataSource));
        checks.put("storageDatabase", databaseReady(storageDataSourceProvider.getIfAvailable()));
        checks.put("redis", redisReady());
        if (order) {
            OrderEventStreamConsumer consumer = orderConsumerProvider.getIfAvailable();
            checks.put("orderConsumerGroup", consumer != null && consumer.isReady());
            checks.put("orderStreamSubscription", listenerRegistry.isReady(ContentType.ORDER.getCode()));
        } else if (payment) {
            PaymentEventStreamConsumer consumer = paymentConsumerProvider.getIfAvailable();
            checks.put("paymentConsumerGroup", consumer != null && consumer.isReady());
            checks.put("paymentStreamSubscription", listenerRegistry.isReady(ContentType.PAYMENT.getCode()));
        }
        return new EventConsumerReadinessVO(checks.values().stream().allMatch(Boolean.TRUE::equals), checks);
    }

    private static boolean databaseReady(DataSource dataSource) {
        if (dataSource == null) return false;
        try (Connection connection = dataSource.getConnection()) {
            return connection.isValid(2);
        } catch (Exception e) {
            return false;
        }
    }

    private boolean redisReady() {
        RedisConnectionFactory factory = redisTemplate.getConnectionFactory();
        if (factory == null) {
            return false;
        }
        try (RedisConnection connection = factory.getConnection()) {
            String pong = connection.ping();
            return pong != null && "PONG".equalsIgnoreCase(pong);
        } catch (Exception e) {
            return false;
        }
    }
}
