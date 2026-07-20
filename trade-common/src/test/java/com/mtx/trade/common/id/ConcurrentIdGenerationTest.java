package com.mtx.trade.common.id;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发 ID 生成测试。
 *
 * <p>覆盖场景：1（10 万 ID 不重复）、2（多线程全局不重复）、
 * 3（多领域并发不重复）、4（全局与领域混合不重复）。</p>
 */
class ConcurrentIdGenerationTest {

    private static final int THREAD_COUNT = 8;
    private static final int IDS_PER_THREAD = 12_500;
    private static final int TOTAL_IDS = THREAD_COUNT * IDS_PER_THREAD; // 100_000

    private SnowflakeIdEngine newEngine() {
        return new SnowflakeIdEngine(
                GlobalIdProperties.DEFAULT_EPOCH, 0, 0, 5, new SystemTimeProvider());
    }

    // ======================== 场景 1：连续 10 万个 ID 不重复 ========================

    @Test
    @DisplayName("场景 1: 全局生成器连续生成 10 万个 ID 不重复")
    void globalGenerator100kUnique() {
        SnowflakeIdEngine engine = newEngine();
        GlobalIdGenerator global = new DefaultGlobalIdGenerator(engine);

        Set<Long> ids = new HashSet<>(TOTAL_IDS);
        long previous = 0;
        for (int i = 0; i < TOTAL_IDS; i++) {
            long id = global.nextId();
            assertTrue(id > 0, "ID 应为正数: " + id);
            assertTrue(ids.add(id), "第 " + i + " 个 ID 重复: " + id);
            if (i > 0) {
                assertTrue(id > previous, "ID 应趋势递增: " + previous + " -> " + id);
            }
            previous = id;
        }
        assertEquals(TOTAL_IDS, ids.size(), "应生成 10 万个不重复的 ID");
    }

    // ======================== 场景 2：多线程调用全局生成器不重复 ========================

    @Test
    @DisplayName("场景 2: 多线程调用全局生成器不重复")
    void multiThreadGlobalNoDuplicates() throws Exception {
        SnowflakeIdEngine engine = newEngine();
        GlobalIdGenerator global = new DefaultGlobalIdGenerator(engine);

        ConcurrentLinkedQueue<Long> allIds = new ConcurrentLinkedQueue<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < IDS_PER_THREAD; i++) {
                        allIds.add(global.nextId());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startLatch.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "线程池应在 60s 内完成");

        Set<Long> unique = new HashSet<>(allIds);
        assertEquals(TOTAL_IDS, allIds.size(), "应生成 10 万个 ID");
        assertEquals(TOTAL_IDS, unique.size(), "多线程生成的 ID 不应有重复");
    }

    // ======================== 场景 3：多个领域生成器并发生成不重复 ========================

    @Test
    @DisplayName("场景 3: 多个领域生成器并发生成不重复")
    void multiDomainConcurrentNoDuplicates() throws Exception {
        SnowflakeIdEngine engine = newEngine();
        DefaultIdGeneratorRegistry registry = new DefaultIdGeneratorRegistry(engine);

        String[] domains = {"order", "payment", "event", "user"};
        ConcurrentLinkedQueue<Long> allIds = new ConcurrentLinkedQueue<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(domains.length);

        int idsPerDomain = TOTAL_IDS / domains.length;
        for (String domain : domains) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    DomainIdGenerator gen = registry.forDomain(domain);
                    for (int i = 0; i < idsPerDomain; i++) {
                        allIds.add(gen.nextId());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startLatch.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

        Set<Long> unique = new HashSet<>(allIds);
        assertEquals(domains.length * idsPerDomain, allIds.size(), "应生成预期数量的 ID");
        assertEquals(domains.length * idsPerDomain, unique.size(),
                "多领域并发生成的 ID 不应有重复（共享同一雪花核心）");
    }

    // ======================== 场景 4：全局与领域混合调用不重复 ========================

    @Test
    @DisplayName("场景 4: 全局生成器与领域生成器混合调用不重复")
    void mixedGlobalAndDomainNoDuplicates() throws Exception {
        SnowflakeIdEngine engine = newEngine();
        DefaultIdGeneratorRegistry registry = new DefaultIdGeneratorRegistry(engine);
        GlobalIdGenerator global = registry.global();

        ConcurrentLinkedQueue<Long> allIds = new ConcurrentLinkedQueue<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(4);

        int idsPerTask = TOTAL_IDS / 4;

        // 线程 1：全局生成器
        pool.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < idsPerTask; i++) {
                    allIds.add(global.nextId());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // 线程 2：order 领域
        pool.submit(() -> {
            try {
                startLatch.await();
                DomainIdGenerator gen = registry.forDomain("order");
                for (int i = 0; i < idsPerTask; i++) {
                    allIds.add(gen.nextId());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // 线程 3：payment 领域
        pool.submit(() -> {
            try {
                startLatch.await();
                DomainIdGenerator gen = registry.forDomain("payment");
                for (int i = 0; i < idsPerTask; i++) {
                    allIds.add(gen.nextId());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // 线程 4：event 领域
        pool.submit(() -> {
            try {
                startLatch.await();
                DomainIdGenerator gen = registry.forDomain("event");
                for (int i = 0; i < idsPerTask; i++) {
                    allIds.add(gen.nextId());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        startLatch.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

        Set<Long> unique = new HashSet<>(allIds);
        assertEquals(TOTAL_IDS, allIds.size(), "应生成 10 万个 ID");
        assertEquals(TOTAL_IDS, unique.size(), "全局与领域混合生成的 ID 不应有重复");

        // 验证所有 ID 为正数
        List<Long> negatives = allIds.stream().filter(id -> id <= 0).collect(Collectors.toList());
        assertTrue(negatives.isEmpty(), "不应有非正数 ID");
    }
}
