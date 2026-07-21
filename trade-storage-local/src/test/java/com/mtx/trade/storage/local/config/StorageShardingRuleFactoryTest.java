package com.mtx.trade.storage.local.config;

import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.HintShardingStrategyConfiguration;
import org.apache.shardingsphere.sharding.api.sharding.hint.HintShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.hint.HintShardingValue;
import org.apache.shardingsphere.sharding.spi.ShardingAlgorithm;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
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
        assertThat(tables.values()).allSatisfy(table -> {
            HintShardingStrategyConfiguration strategy =
                    (HintShardingStrategyConfiguration) table.getTableShardingStrategy();
            assertThat(strategy.getShardingAlgorithmName()).isEqualTo("storage_sha256");
        });
    }

    @Test
    void shouldUseFullUnsignedSha256Modulo() {
        byte[] sha256 = new byte[32];
        sha256[0] = (byte) 0xFF;
        sha256[31] = 0x7F;

        int expected = new BigInteger(1, sha256).mod(BigInteger.valueOf(100)).intValue();

        assertThat(Sha256ShardingAlgorithm.shardIndex(sha256)).isEqualTo(expected);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void shouldRouteShaBucketHintToTwoDigitPhysicalTable() {
        byte[] sha256 = new byte[32];
        sha256[31] = 99;
        ShardingRuleConfiguration rule = StorageShardingRuleFactory.create();
        HintShardingAlgorithm algorithm = (HintShardingAlgorithm) TypedSPILoader.getService(
                ShardingAlgorithm.class,
                "CLASS_BASED",
                rule.getShardingAlgorithms().get("storage_sha256").getProps());
        HintShardingValue shardingValue = new HintShardingValue(
                "trade_storage", "", List.of(Sha256ShardingAlgorithm.shardIndex(sha256)));

        java.util.Collection<String> targets = algorithm.doSharding(
                List.of("trade_storage_00", "trade_storage_01", "trade_storage_99"), shardingValue);

        assertThat(targets).containsExactly("trade_storage_99");
    }

    @Test
    void shouldDistributeSha256SamplesAcrossAllBuckets() throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < 10_000; i++) {
            byte[] sha256 = digest.digest(Integer.toString(i).getBytes(StandardCharsets.UTF_8));
            counts.merge(Sha256ShardingAlgorithm.shardIndex(sha256), 1, Integer::sum);
        }

        assertThat(counts).hasSize(100);
        assertThat(counts.values()).allMatch(each -> each >= 65 && each <= 135);
    }
}
