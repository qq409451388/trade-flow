package com.mtx.trade.storage.local.config;

import org.apache.shardingsphere.sharding.api.sharding.hint.HintShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.hint.HintShardingValue;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;

/** 使用完整 SHA-256 无符号值对固定100个虚拟桶取模。 */
public final class Sha256ShardingAlgorithm implements HintShardingAlgorithm<Comparable<?>> {

    private static final int SHA256_LENGTH = 32;
    private static final BigInteger SHARD_COUNT = BigInteger.valueOf(StorageShardingRuleFactory.SHARD_COUNT);

    @Override
    public Collection<String> doSharding(Collection<String> availableTargetNames,
                                         HintShardingValue<Comparable<?>> shardingValue) {
        if (shardingValue.getValues().size() != 1) {
            throw new IllegalArgumentException("Storage 路由必须且只能提供一个 SHA-256 分片桶");
        }
        Comparable<?> value = shardingValue.getValues().iterator().next();
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException("Storage Hint 分片值必须为数字桶号");
        }
        int shard = number.intValue();
        if (shard < 0 || shard >= StorageShardingRuleFactory.SHARD_COUNT) {
            throw new IllegalArgumentException("Storage Hint 分片桶超出范围: " + shard);
        }
        String suffix = String.format("_%02d", shard);
        String target = availableTargetNames.stream()
                .filter(each -> each.endsWith(suffix))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("找不到 Storage 物理分表: " + suffix));
        return List.of(target);
    }

    static int shardIndex(byte[] sha256) {
        if (sha256 == null) {
            throw new IllegalArgumentException("Storage 分片 SHA-256 不能为空");
        }
        if (sha256.length != SHA256_LENGTH) {
            throw new IllegalArgumentException("Storage 分片 SHA-256 必须为32字节");
        }
        return new BigInteger(1, sha256).mod(SHARD_COUNT).intValue();
    }
}
