package com.mtx.trade.pipeline.config;

import org.apache.shardingsphere.sharding.api.sharding.hint.HintShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.hint.HintShardingValue;

import java.util.Collection;
import java.util.Set;

/** 支付父子表强制显式年度路由，缺少 Hint 时禁止广播。 */
public final class PipelineRequiredYearHintShardingAlgorithm implements HintShardingAlgorithm<Comparable<?>> {

    @Override
    public Collection<String> doSharding(
            Collection<String> availableTargetNames,
            HintShardingValue<Comparable<?>> shardingValue) {
        Set<Integer> years = PipelineShardingValueSupport.yearsOf(shardingValue.getValues());
        if (years.size() != 1) {
            throw new IllegalArgumentException("Payment operation requires exactly one routeYear hint");
        }
        int year = years.iterator().next();
        return java.util.List.of(PipelineShardingValueSupport.requireSingleTarget(
                availableTargetNames, "_" + year));
    }
}
