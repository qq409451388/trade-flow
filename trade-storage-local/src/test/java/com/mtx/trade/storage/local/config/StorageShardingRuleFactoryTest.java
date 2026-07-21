package com.mtx.trade.storage.local.config;

import org.apache.shardingsphere.infra.datanode.DataNodeInfo;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.StandardShardingAlgorithm;
import org.apache.shardingsphere.sharding.spi.ShardingAlgorithm;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class StorageShardingRuleFactoryTest {

    @Test
    void shouldDefineFixedStorageShardingContract() {
        ShardingRuleConfiguration rule = StorageShardingRuleFactory.create();
        Map<String, ShardingTableRuleConfiguration> tables = rule.getTables().stream()
                .collect(Collectors.toMap(ShardingTableRuleConfiguration::getLogicTable, Function.identity()));

        assertThat(tables).containsOnlyKeys("trade_storage", "trade_storage_blob");
        assertThat(tables.values()).allSatisfy(table -> {
            assertThat(table.getActualDataNodes().split(",")).hasSize(100);
            assertThat(table.getActualDataNodes()).contains(
                    "storage_mysql." + table.getLogicTable() + "_00",
                    "storage_mysql." + table.getLogicTable() + "_99");
        });
        assertThat(rule.getBindingTableGroups()).singleElement().satisfies(binding ->
                assertThat(binding.getReference()).isEqualTo("trade_storage,trade_storage_blob"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldRouteByIdModuloWithTwoDigitSuffix() {
        ShardingRuleConfiguration rule = StorageShardingRuleFactory.create();
        StandardShardingAlgorithm<Comparable<?>> algorithm = (StandardShardingAlgorithm<Comparable<?>>)
                TypedSPILoader.getService(
                        ShardingAlgorithm.class,
                        "INLINE",
                        rule.getShardingAlgorithms().get("trade_storage_inline").getProps());

        String target = algorithm.doSharding(
                List.of("trade_storage_00", "trade_storage_01", "trade_storage_99"),
                new PreciseShardingValue<>(
                        "trade_storage", "id", new DataNodeInfo("", 2, '0'), 101L));

        assertThat(target).isEqualTo("trade_storage_01");
    }
}
