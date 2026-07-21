package com.mtx.trade.pipeline.config;

import org.apache.shardingsphere.sharding.api.sharding.hint.HintShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.hint.HintShardingValue;

import java.util.Collection;
import java.util.Set;

/** 为自身没有业务时间字段的订单子表按显式年份 Hint 路由。 */
public final class PipelineYearHintShardingAlgorithm implements HintShardingAlgorithm<Comparable<?>> {

    @Override
    public Collection<String> doSharding(
            Collection<String> availableTargetNames,
            HintShardingValue<Comparable<?>> shardingValue) {
        Set<Integer> years = PipelineShardingValueSupport.yearsOf(shardingValue.getValues());
        if (years.isEmpty()) {
            return availableTargetNames;
        }
        Collection<String> targets = availableTargetNames.stream()
                .filter(target -> years.stream().anyMatch(year -> target.endsWith("_" + year)))
                .toList();
        if (targets.isEmpty()) {
            throw new IllegalArgumentException("No pipeline physical table matches hinted year " + years);
        }
        return targets;
    }
}
