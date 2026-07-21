package com.mtx.trade.common.id;

import com.mtx.trade.common.id.exception.ClockBackwardException;
import com.mtx.trade.common.id.exception.InvalidDatacenterIdException;
import com.mtx.trade.common.id.exception.InvalidWorkerIdException;
import com.mtx.trade.common.id.exception.TimestampOverflowException;

/**
 * 雪花算法 ID 引擎。
 *
 * <p>位结构（从高位到低位）：
 * <pre>
 *   1 位符号位（固定 0，保证正数）
 *  41 位时间戳（毫秒，自 epoch 起）
 *   5 位数据中心 ID（0~31）
 *   5 位机器 ID（0~31）
 *  12 位毫秒内序列号（0~4095）
 * </pre>
 *
 * <p>线程安全：{@link #nextId()} 使用 {@code synchronized} 保证正确性。
 * 默认领域生成器共享全局引擎，配置为独立的领域拥有单独的引擎实例。</p>
 *
 * <h3>时钟回拨</h3>
 * <ul>
 *   <li>回拨不超过 {@code maxClockBackwardMs} 时，等待时间追平后继续；</li>
 *   <li>超过阈值时，拒绝生成 ID 并抛出 {@link ClockBackwardException}；</li>
 *   <li>异常中包含当前时间、上次时间、回拨毫秒数、datacenterId、workerId。</li>
 * </ul>
 */
public class SnowflakeIdEngine {

    // ---- 位长度 ----
    private static final long SEQUENCE_BITS = 12L;
    private static final long WORKER_ID_BITS = 5L;
    private static final long DATACENTER_ID_BITS = 5L;
    private static final long TIMESTAMP_BITS = 41L;

    // ---- 最大值 ----
    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);          // 4095
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);          // 31
    private static final long MAX_DATACENTER_ID = ~(-1L << DATACENTER_ID_BITS);  // 31
    private static final long MAX_TIMESTAMP = ~(-1L << TIMESTAMP_BITS);          // 2^41 - 1

    // ---- 位移 ----
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;                                   // 12
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;               // 17
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS; // 22

    private final long epoch;
    private final long datacenterId;
    private final long workerId;
    private final long maxClockBackwardMs;
    private final TimeProvider timeProvider;

    private long lastTimestamp = -1L;
    private long sequence = 0L;

    /**
     * @param epoch               起始时间戳（毫秒），固定后不得修改
     * @param datacenterId        数据中心 ID（0~31）
     * @param workerId            机器 ID（0~31）
     * @param maxClockBackwardMs   可容忍的最大时钟回拨毫秒数
     * @param timeProvider        时间提供者
     * @throws InvalidDatacenterIdException 如果 datacenterId 越界
     * @throws InvalidWorkerIdException     如果 workerId 越界
     */
    public SnowflakeIdEngine(long epoch, long datacenterId, long workerId,
                             long maxClockBackwardMs, TimeProvider timeProvider) {
        if (datacenterId < 0 || datacenterId > MAX_DATACENTER_ID) {
            throw new InvalidDatacenterIdException(datacenterId, MAX_DATACENTER_ID);
        }
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new InvalidWorkerIdException(workerId, MAX_WORKER_ID);
        }
        if (maxClockBackwardMs < 0) {
            throw new IllegalArgumentException("maxClockBackwardMs must be >= 0");
        }
        if (timeProvider == null) {
            throw new IllegalArgumentException("timeProvider must not be null");
        }
        this.epoch = epoch;
        this.datacenterId = datacenterId;
        this.workerId = workerId;
        this.maxClockBackwardMs = maxClockBackwardMs;
        this.timeProvider = timeProvider;
    }

    /**
     * 生成下一个全局唯一 ID。
     *
     * <p>线程安全（{@code synchronized}），可被多个领域生成器并发调用。</p>
     *
     * @return 正数 {@code long}
     * @throws ClockBackwardException    时钟回拨超过阈值
     * @throws TimestampOverflowException 时间戳超过 41 位范围
     */
    public synchronized long nextId() {
        long now = timeProvider.currentTimeMillis();
        long currentTimestamp = now - epoch;

        // ---- 时钟回拨处理 ----
        if (currentTimestamp < lastTimestamp) {
            long backwardMs = lastTimestamp - currentTimestamp;
            if (backwardMs <= maxClockBackwardMs) {
                // 回拨在阈值内，等待时间追平
                try {
                    Thread.sleep(backwardMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ClockBackwardException(currentTimestamp, lastTimestamp,
                            backwardMs, datacenterId, workerId);
                }
                // 重新读取时间
                now = timeProvider.currentTimeMillis();
                currentTimestamp = now - epoch;
                if (currentTimestamp < lastTimestamp) {
                    backwardMs = lastTimestamp - currentTimestamp;
                    throw new ClockBackwardException(currentTimestamp, lastTimestamp,
                            backwardMs, datacenterId, workerId);
                }
            } else {
                // 回拨超过阈值，拒绝生成
                throw new ClockBackwardException(currentTimestamp, lastTimestamp,
                        backwardMs, datacenterId, workerId);
            }
        }

        // ---- 同一毫秒内序列自增 ----
        if (currentTimestamp == lastTimestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0L) {
                // 序列耗尽（已达 4095+1），等待下一毫秒
                currentTimestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            // 新毫秒，重置序列
            sequence = 0L;
        }

        // ---- 时间戳溢出检查 ----
        if (currentTimestamp > MAX_TIMESTAMP) {
            throw new TimestampOverflowException(currentTimestamp, MAX_TIMESTAMP);
        }

        lastTimestamp = currentTimestamp;

        return (currentTimestamp << TIMESTAMP_SHIFT)
                | (datacenterId << DATACENTER_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    /**
     * 自旋等待到下一毫秒。
     *
     * @param lastTimestamp 上次时间戳（epoch 后的相对毫秒）
     * @return 新的（更大的）时间戳
     */
    private long waitNextMillis(long lastTimestamp) {
        long timestamp = timeProvider.currentTimeMillis() - epoch;
        while (timestamp <= lastTimestamp) {
            timestamp = timeProvider.currentTimeMillis() - epoch;
        }
        return timestamp;
    }

    // ---- 只读属性（用于监控/测试） ----

    public long getEpoch() {
        return epoch;
    }

    public long getDatacenterId() {
        return datacenterId;
    }

    public long getWorkerId() {
        return workerId;
    }

    public long getMaxClockBackwardMs() {
        return maxClockBackwardMs;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }
}
