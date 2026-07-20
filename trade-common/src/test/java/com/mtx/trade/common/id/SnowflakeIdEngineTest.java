package com.mtx.trade.common.id;

import com.mtx.trade.common.id.exception.ClockBackwardException;
import com.mtx.trade.common.id.exception.InvalidDatacenterIdException;
import com.mtx.trade.common.id.exception.InvalidWorkerIdException;
import com.mtx.trade.common.id.exception.TimestampOverflowException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link SnowflakeIdEngine} 单元测试。
 *
 * <p>覆盖场景：8（datacenterId 越界）、9（workerId 越界）、
 * 10（小幅时钟回拨恢复）、11（大幅时钟回拨异常）、12（序列耗尽进入下一毫秒）。</p>
 */
class SnowflakeIdEngineTest {

    private static final long EPOCH = GlobalIdProperties.DEFAULT_EPOCH;

    // ======================== 场景 8：datacenterId 越界 ========================

    @Nested
    @DisplayName("场景 8: datacenterId 越界启动失败")
    class DatacenterIdValidation {

        @Test
        @DisplayName("datacenterId = 32 越界")
        void datacenterIdTooLarge() {
            InvalidDatacenterIdException ex = assertThrows(
                    InvalidDatacenterIdException.class,
                    () -> new SnowflakeIdEngine(EPOCH, 32, 0, 5, new SystemTimeProvider())
            );
            assertEquals(32, ex.getProvided());
            assertEquals(31, ex.getMaxAllowed());
        }

        @Test
        @DisplayName("datacenterId = -1 越界")
        void datacenterIdNegative() {
            assertThrows(
                    InvalidDatacenterIdException.class,
                    () -> new SnowflakeIdEngine(EPOCH, -1, 0, 5, new SystemTimeProvider())
            );
        }
    }

    // ======================== 场景 9：workerId 越界 ========================

    @Nested
    @DisplayName("场景 9: workerId 越界启动失败")
    class WorkerIdValidation {

        @Test
        @DisplayName("workerId = 32 越界")
        void workerIdTooLarge() {
            InvalidWorkerIdException ex = assertThrows(
                    InvalidWorkerIdException.class,
                    () -> new SnowflakeIdEngine(EPOCH, 0, 32, 5, new SystemTimeProvider())
            );
            assertEquals(32, ex.getProvided());
            assertEquals(31, ex.getMaxAllowed());
        }

        @Test
        @DisplayName("workerId = -1 越界")
        void workerIdNegative() {
            assertThrows(
                    InvalidWorkerIdException.class,
                    () -> new SnowflakeIdEngine(EPOCH, 0, -1, 5, new SystemTimeProvider())
            );
        }
    }

    // ======================== 场景 10：小幅时钟回拨恢复 ========================

    @Test
    @DisplayName("场景 10: 小幅时钟回拨（在阈值内）可以恢复")
    void smallClockBackwardRecovers() throws Exception {
        long now = System.currentTimeMillis();
        ControllableTimeProvider tp = new ControllableTimeProvider(now);
        SnowflakeIdEngine engine = new SnowflakeIdEngine(EPOCH, 1, 1, 100, tp);

        // 生成一个 ID，记录 lastTimestamp
        long firstId = engine.nextId();
        long lastTs = engine.getLastTimestamp();

        // 时钟回拨 50ms（在阈值 100ms 内，引擎会 sleep(50ms) 等待）
        tp.set(now - 50);

        // 另一个线程在 sleep 期间推进时间（10ms 后推进，远早于 50ms sleep 结束）
        Thread advancer = new Thread(() -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            tp.set(now + 10);
        });
        advancer.start();

        // 调用 nextId：检测到回拨 50ms（<= 100），sleep(50ms)，期间 advancer 已将时间推进到 now+10
        long secondId = engine.nextId();
        advancer.join();

        assertTrue(secondId > firstId, "回拨恢复后生成的 ID 应大于之前的 ID");
        assertNotEquals(lastTs, engine.getLastTimestamp(), "lastTimestamp 应已推进");
    }

    // ======================== 场景 11：大幅时钟回拨异常 ========================

    @Test
    @DisplayName("场景 11: 大幅时钟回拨（超过阈值）抛出异常")
    void largeClockBackwardThrows() {
        long now = System.currentTimeMillis();
        ControllableTimeProvider tp = new ControllableTimeProvider(now);
        SnowflakeIdEngine engine = new SnowflakeIdEngine(EPOCH, 2, 3, 5, tp);

        // 先正常生成一个 ID
        engine.nextId();

        // 时钟回拨 100ms（超过阈值 5ms）
        tp.set(now - 100);

        ClockBackwardException ex = assertThrows(
                ClockBackwardException.class,
                engine::nextId
        );
        assertEquals(100, ex.getBackwardMs(), "回拨毫秒数应为 100");
        assertEquals(2, ex.getDatacenterId(), "异常中应包含 datacenterId");
        assertEquals(3, ex.getWorkerId(), "异常中应包含 workerId");
        assertTrue(ex.getLastTimestamp() > ex.getCurrentTimestamp(), "lastTimestamp 应大于 currentTimestamp");
    }

    // ======================== 场景 12：单毫秒序列耗尽 ========================

    @Test
    @DisplayName("场景 12: 单毫秒序列耗尽后进入下一毫秒")
    void sequenceExhaustionMovesToNextMs() throws Exception {
        long now = System.currentTimeMillis();
        ControllableTimeProvider tp = new ControllableTimeProvider(now);
        SnowflakeIdEngine engine = new SnowflakeIdEngine(EPOCH, 0, 0, 100, tp);

        // 在同一毫秒内生成 4096 个 ID（sequence 0~4095）
        long[] ids = new long[4096];
        for (int i = 0; i < 4096; i++) {
            ids[i] = engine.nextId();
        }

        // 验证这 4096 个 ID 的 timestamp 部分相同
        long firstTs = extractTimestamp(ids[0]);
        for (int i = 1; i < 4096; i++) {
            assertEquals(firstTs, extractTimestamp(ids[i]),
                    "前 4096 个 ID 应在同一毫秒");
            assertTrue(ids[i] > ids[i - 1], "同一毫秒内 ID 递增");
        }

        // 第 4097 个 ID：sequence 耗尽，waitNextMillis 自旋等待
        Thread advancer = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            tp.advance(1); // 推进 1ms
        });
        advancer.start();

        long id4097 = engine.nextId();
        advancer.join();

        long ts4097 = extractTimestamp(id4097);
        assertTrue(ts4097 > firstTs, "第 4097 个 ID 应在下一毫秒");
        assertTrue(id4097 > ids[4095], "第 4097 个 ID 应大于最后一个同毫秒 ID");
    }

    // ======================== 辅助方法 ========================

    /**
     * 从雪花 ID 中提取时间戳部分（epoch 后的相对毫秒）。
     */
    private static long extractTimestamp(long id) {
        return id >>> 22;
    }
}
