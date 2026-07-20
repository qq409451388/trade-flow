package com.mtx.trade.common.storage.config;

import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.StandardShardingAlgorithm;
import org.apache.shardingsphere.sharding.spi.ShardingAlgorithm;
import org.apache.shardingsphere.infra.datanode.DataNodeInfo;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class StorageShardingAutoConfigurationTest {

    private final StorageShardingAutoConfiguration configuration = new StorageShardingAutoConfiguration();

    @Test
    void shouldDefineFixedStorageShardingContract() {
        ShardingRuleConfiguration rule = configuration.storageShardingRuleConfiguration();
        Map<String, ShardingTableRuleConfiguration> tables = rule.getTables().stream()
                .collect(Collectors.toMap(ShardingTableRuleConfiguration::getLogicTable, Function.identity()));

        assertThat(tables).containsOnlyKeys("trade_storage", "trade_storage_blob");
        assertThat(tables.values()).allSatisfy(table -> {
            assertThat(table.getActualDataNodes().split(",")).hasSize(100);
            assertThat(table.getActualDataNodes()).contains(
                    "ds_0." + table.getLogicTable() + "_00",
                    "ds_0." + table.getLogicTable() + "_99");
        });
        assertThat(rule.getShardingAlgorithms()).containsOnlyKeys(
                "trade_storage_inline", "trade_storage_blob_inline");
        assertThat(rule.getBindingTableGroups()).singleElement().satisfies(binding ->
                assertThat(binding.getReference()).isEqualTo("trade_storage,trade_storage_blob"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldRouteByIdModuloWithTwoDigitSuffix() {
        ShardingRuleConfiguration rule = configuration.storageShardingRuleConfiguration();
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
