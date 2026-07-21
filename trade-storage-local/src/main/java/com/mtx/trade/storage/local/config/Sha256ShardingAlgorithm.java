package com.mtx.trade.storage.local.config;

import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.RangeShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.StandardShardingAlgorithm;

import java.math.BigInteger;
import java.util.Collection;

/** 使用完整 SHA-256 无符号值对固定100个虚拟桶取模。 */
public final class Sha256ShardingAlgorithm implements StandardShardingAlgorithm<Comparable<?>> {

    private static final int SHA256_LENGTH = 32;
    private static final BigInteger SHARD_COUNT = BigInteger.valueOf(StorageShardingRuleFactory.SHARD_COUNT);

    @Override
    public String doSharding(Collection<String> availableTargetNames,
                             PreciseShardingValue<Comparable<?>> shardingValue) {
        int shard = shardIndex(shardingValue.getValue());
        String suffix = String.format("_%02d", shard);
        return availableTargetNames.stream()
                .filter(each -> each.endsWith(suffix))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("找不到 Storage 物理分表: " + suffix));
    }

    @Override
    public Collection<String> doSharding(Collection<String> availableTargetNames,
                                         RangeShardingValue<Comparable<?>> shardingValue) {
        return availableTargetNames;
    }

    static int shardIndex(Object value) {
        byte[] sha256 = toBytes(value);
        if (sha256.length != SHA256_LENGTH) {
            throw new IllegalArgumentException("Storage 分片 SHA-256 必须为32字节");
        }
        return new BigInteger(1, sha256).mod(SHARD_COUNT).intValue();
    }

    private static byte[] toBytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof String hex && hex.length() == SHA256_LENGTH * 2) {
            return java.util.HexFormat.of().parseHex(hex);
        }
        throw new IllegalArgumentException("Storage 分片值必须为32字节 SHA-256");
    }
}
