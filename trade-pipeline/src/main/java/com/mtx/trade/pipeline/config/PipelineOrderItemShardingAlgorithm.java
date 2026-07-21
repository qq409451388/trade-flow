package com.mtx.trade.pipeline.config;

import com.google.common.collect.Range;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingValue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 订单商品明细按 {@code item_create_time.year + floorMod(order_no, shardCount)} 路由。 */
public final class PipelineOrderItemShardingAlgorithm
        implements ComplexKeysShardingAlgorithm<Comparable<?>> {

    private static final Pattern TARGET_PATTERN = Pattern.compile(".*_(\\d{4})_(\\d{2})$");

    @Override
    public Collection<String> doSharding(
            Collection<String> availableTargetNames,
            ComplexKeysShardingValue<Comparable<?>> shardingValue) {
        Map<String, Collection<Comparable<?>>> preciseValues =
                shardingValue.getColumnNameAndShardingValuesMap();
        Set<Integer> preciseYears = PipelineShardingValueSupport.yearsOf(
                preciseValues.get("item_create_time"));
        Set<Integer> shards = shardsOf(preciseValues.get("order_no"), shardCount(availableTargetNames));
        Range<Comparable<?>> timeRange = shardingValue.getColumnNameAndRangeValuesMap()
                .get("item_create_time");

        Collection<String> targets = availableTargetNames.stream()
                .filter(target -> matches(target, preciseYears, timeRange, shards))
                .toList();
        if (targets.isEmpty()) {
            throw new IllegalArgumentException("No oms_order_item physical table matches sharding values");
        }
        return targets;
    }

    private static boolean matches(
            String target,
            Set<Integer> preciseYears,
            Range<Comparable<?>> timeRange,
            Set<Integer> shards) {
        Matcher matcher = TARGET_PATTERN.matcher(target);
        if (!matcher.matches()) {
            return false;
        }
        int year = Integer.parseInt(matcher.group(1));
        int shard = Integer.parseInt(matcher.group(2));
        boolean yearMatches = preciseYears.isEmpty()
                ? PipelineShardingValueSupport.yearMatchesRange(year, timeRange)
                : preciseYears.contains(year);
        return yearMatches && (shards.isEmpty() || shards.contains(shard));
    }

    private static Set<Integer> shardsOf(Collection<Comparable<?>> orderNos, int shardCount) {
        if (orderNos == null || orderNos.isEmpty()) {
            return Collections.emptySet();
        }
        Set<Integer> result = new LinkedHashSet<>();
        for (Comparable<?> orderNo : orderNos) {
            if (!(orderNo instanceof Number number)) {
                throw new IllegalArgumentException("order_no sharding value must be numeric: " + orderNo);
            }
            result.add(Math.floorMod(number.longValue(), shardCount));
        }
        return result;
    }

    private static int shardCount(Collection<String> availableTargetNames) {
        int maxShard = availableTargetNames.stream()
                .map(TARGET_PATTERN::matcher)
                .filter(Matcher::matches)
                .mapToInt(matcher -> Integer.parseInt(matcher.group(2)))
                .max()
                .orElseThrow(() -> new IllegalArgumentException("No oms_order_item physical tables configured"));
        return maxShard + 1;
    }
}
