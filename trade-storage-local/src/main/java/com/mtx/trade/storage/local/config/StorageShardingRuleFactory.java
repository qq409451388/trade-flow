package com.mtx.trade.storage.local.config;

import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableReferenceRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;

import java.util.Properties;

/** 构造 storage 固定的 100 个虚拟分片规则。 */
public final class StorageShardingRuleFactory {

    public static final int SHARD_COUNT = 100;
    private static final String DATA_SOURCE_NAME = "storage_mysql";
    private static final String SHARD_COLUMN = "id";
    private static final String[] TABLES = {"trade_storage", "trade_storage_blob"};

    private StorageShardingRuleFactory() {
    }

    public static ShardingRuleConfiguration create() {
        ShardingRuleConfiguration rule = new ShardingRuleConfiguration();
        for (String table : TABLES) {
            rule.getTables().add(tableRule(table));
            Properties algorithmProperties = new Properties();
            algorithmProperties.setProperty(
                    "algorithm-expression",
                    table + "_${String.format('%02d', " + SHARD_COLUMN + " % " + SHARD_COUNT + ")}");
            rule.getShardingAlgorithms().put(
                    table + "_inline",
                    new AlgorithmConfiguration("INLINE", algorithmProperties));
        }
        rule.getBindingTableGroups().add(new ShardingTableReferenceRuleConfiguration(
                "storage_binding", "trade_storage,trade_storage_blob"));
        return rule;
    }

    private static ShardingTableRuleConfiguration tableRule(String table) {
        StringBuilder actualDataNodes = new StringBuilder();
        for (int shard = 0; shard < SHARD_COUNT; shard++) {
            if (shard > 0) {
                actualDataNodes.append(',');
            }
            actualDataNodes.append(DATA_SOURCE_NAME)
                    .append('.').append(table).append('_').append(String.format("%02d", shard));
        }
        ShardingTableRuleConfiguration tableRule = new ShardingTableRuleConfiguration(table, actualDataNodes.toString());
        tableRule.setTableShardingStrategy(
                new StandardShardingStrategyConfiguration(SHARD_COLUMN, table + "_inline"));
        return tableRule;
    }
}
