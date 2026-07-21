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
    private final DataSource storageDataSource;
    private final StringRedisTemplate redisTemplate;
    private final ObjectProvider<OrderEventStreamConsumer> consumerProvider;

    public EventConsumerReadinessService(
            @Qualifier("pipelineActualDataSource") DataSource pipelineDataSource,
            @Qualifier("storageActualDataSource") DataSource storageDataSource,
            StringRedisTemplate redisTemplate,
            ObjectProvider<OrderEventStreamConsumer> consumerProvider) {
        this.pipelineDataSource = pipelineDataSource;
        this.storageDataSource = storageDataSource;
        this.redisTemplate = redisTemplate;
        this.consumerProvider = consumerProvider;
    }

    public EventConsumerReadinessVO check(Integer contentType) {
        Map<String, Boolean> checks = new LinkedHashMap<>();
        checks.put("contentTypeSupported", contentType != null && contentType == ContentType.ORDER.getCode());
        checks.put("pipelineDatabase", databaseReady(pipelineDataSource));
        checks.put("storageDatabase", databaseReady(storageDataSource));
        checks.put("redis", redisReady());
        OrderEventStreamConsumer consumer = consumerProvider.getIfAvailable();
        checks.put("orderConsumerGroup", consumer != null && consumer.isReady());
        return new EventConsumerReadinessVO(checks.values().stream().allMatch(Boolean.TRUE::equals), checks);
    }

    private static boolean databaseReady(DataSource dataSource) {
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
