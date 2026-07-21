package com.mtx.trade.pipeline.config;

import org.apache.shardingsphere.infra.algorithm.core.config.AlgorithmConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.ComplexShardingStrategyConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.HintShardingStrategyConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/** 根据已建年份构造 Pipeline 订单分表规则。 */
public final class PipelineShardingRuleFactory {

    public static final String DATA_SOURCE_NAME = "pipeline_mysql";
    public static final String ORDER_TABLE = "oms_order";
    public static final String ORDER_ITEM_TABLE = "oms_order_item";
    public static final String ORDER_ITEM_SPEC_TABLE = "oms_order_item_spec";
    public static final String ORDER_PACKAGE_ITEM_TABLE = "oms_order_package_item";
    public static final String PAYMENT_TABLE = "oms_payment";
    public static final String PAYMENT_ACCOUNT_TABLE = "oms_payment_account";

    private static final String YEAR_ALGORITHM = "pipeline_order_year";
    private static final String YEAR_ORDER_ALGORITHM = "pipeline_order_item_year_hash";
    private static final String YEAR_HINT_ALGORITHM = "pipeline_order_year_hint";
    private static final String REQUIRED_YEAR_HINT_ALGORITHM = "pipeline_required_year_hint";

    private PipelineShardingRuleFactory() {
    }

    public static ShardingRuleConfiguration create(PipelineShardingProperties properties) {
        List<Integer> years = normalizedYears(properties.getYears());
        int shardCount = properties.getOrderItemShardCount();
        if (shardCount <= 0) {
            throw new IllegalArgumentException("trade.pipeline.sharding.order-item-shard-count must be positive");
        }

        ShardingRuleConfiguration rule = new ShardingRuleConfiguration();
        rule.getTables().add(yearTableRule(ORDER_TABLE, "order_create_time", years));
        rule.getTables().add(orderItemTableRule(years, shardCount));
        rule.getTables().add(yearHintTableRule(ORDER_ITEM_SPEC_TABLE, years));
        rule.getTables().add(yearTableRule(ORDER_PACKAGE_ITEM_TABLE, "item_create_time", years));
        rule.getTables().add(requiredYearHintTableRule(PAYMENT_TABLE, years));
        rule.getTables().add(requiredYearHintTableRule(PAYMENT_ACCOUNT_TABLE, years));

        rule.getShardingAlgorithms().put(YEAR_ALGORITHM,
                classBasedAlgorithm("STANDARD", PipelineYearShardingAlgorithm.class));
        rule.getShardingAlgorithms().put(YEAR_ORDER_ALGORITHM,
                classBasedAlgorithm("COMPLEX", PipelineOrderItemShardingAlgorithm.class));
        rule.getShardingAlgorithms().put(YEAR_HINT_ALGORITHM,
                classBasedAlgorithm("HINT", PipelineYearHintShardingAlgorithm.class));
        rule.getShardingAlgorithms().put(REQUIRED_YEAR_HINT_ALGORITHM,
                classBasedAlgorithm("HINT", PipelineRequiredYearHintShardingAlgorithm.class));
        return rule;
    }

    private static ShardingTableRuleConfiguration yearTableRule(
            String logicTable, String shardingColumn, List<Integer> years) {
        ShardingTableRuleConfiguration tableRule = new ShardingTableRuleConfiguration(
                logicTable, yearDataNodes(logicTable, years));
        tableRule.setTableShardingStrategy(
                new StandardShardingStrategyConfiguration(shardingColumn, YEAR_ALGORITHM));
        return tableRule;
    }

    private static ShardingTableRuleConfiguration orderItemTableRule(List<Integer> years, int shardCount) {
        StringBuilder nodes = new StringBuilder();
        for (Integer year : years) {
            for (int shard = 0; shard < shardCount; shard++) {
                appendNode(nodes, ORDER_ITEM_TABLE + '_' + year + '_' + String.format("%02d", shard));
            }
        }
        ShardingTableRuleConfiguration tableRule = new ShardingTableRuleConfiguration(
                ORDER_ITEM_TABLE, nodes.toString());
        tableRule.setTableShardingStrategy(new ComplexShardingStrategyConfiguration(
                "item_create_time,order_no", YEAR_ORDER_ALGORITHM));
        return tableRule;
    }

    private static ShardingTableRuleConfiguration yearHintTableRule(String logicTable, List<Integer> years) {
        ShardingTableRuleConfiguration tableRule = new ShardingTableRuleConfiguration(
                logicTable, yearDataNodes(logicTable, years));
        tableRule.setTableShardingStrategy(new HintShardingStrategyConfiguration(YEAR_HINT_ALGORITHM));
        return tableRule;
    }

    private static ShardingTableRuleConfiguration requiredYearHintTableRule(
            String logicTable, List<Integer> years) {
        ShardingTableRuleConfiguration tableRule = new ShardingTableRuleConfiguration(
                logicTable, yearDataNodes(logicTable, years));
        tableRule.setTableShardingStrategy(
                new HintShardingStrategyConfiguration(REQUIRED_YEAR_HINT_ALGORITHM));
        return tableRule;
    }

    private static String yearDataNodes(String logicTable, List<Integer> years) {
        StringBuilder nodes = new StringBuilder();
        years.forEach(year -> appendNode(nodes, logicTable + '_' + year));
        return nodes.toString();
    }

    private static void appendNode(StringBuilder nodes, String table) {
        if (!nodes.isEmpty()) {
            nodes.append(',');
        }
        nodes.append(DATA_SOURCE_NAME).append('.').append(table);
    }

    private static AlgorithmConfiguration classBasedAlgorithm(
            String strategy, Class<?> algorithmClass) {
        Properties properties = new Properties();
        properties.setProperty("strategy", strategy);
        properties.setProperty("algorithmClassName", algorithmClass.getName());
        return new AlgorithmConfiguration("CLASS_BASED", properties);
    }

    private static List<Integer> normalizedYears(List<Integer> configuredYears) {
        if (configuredYears == null || configuredYears.isEmpty()) {
            throw new IllegalArgumentException("trade.pipeline.sharding.years must not be empty");
        }
        Set<Integer> years = new LinkedHashSet<>(configuredYears);
        if (years.stream().anyMatch(year -> year == null || year < 2000 || year > 9999)) {
            throw new IllegalArgumentException("trade.pipeline.sharding.years contains invalid year");
        }
        return years.stream().sorted().toList();
    }
}
