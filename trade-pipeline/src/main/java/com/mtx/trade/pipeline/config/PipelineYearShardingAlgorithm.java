package com.mtx.trade.pipeline.config;

import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.RangeShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.StandardShardingAlgorithm;

import java.util.Collection;

/** 按业务时间年份路由到 {@code logic_table_YYYY}。 */
public final class PipelineYearShardingAlgorithm implements StandardShardingAlgorithm<Comparable<?>> {

    @Override
    public String doSharding(
            Collection<String> availableTargetNames,
            PreciseShardingValue<Comparable<?>> shardingValue) {
        int year = PipelineShardingValueSupport.yearOf(shardingValue.getValue());
        return PipelineShardingValueSupport.requireSingleTarget(availableTargetNames, "_" + year);
    }

    @Override
    public Collection<String> doSharding(
            Collection<String> availableTargetNames,
            RangeShardingValue<Comparable<?>> shardingValue) {
        Collection<String> targets = availableTargetNames.stream()
                .filter(target -> PipelineShardingValueSupport.yearMatchesRange(
                        Integer.parseInt(target.substring(target.length() - 4)),
                        shardingValue.getValueRange()))
                .toList();
        if (targets.isEmpty()) {
            throw new IllegalArgumentException("No pipeline physical table matches requested year range");
        }
        return targets;
    }
}
