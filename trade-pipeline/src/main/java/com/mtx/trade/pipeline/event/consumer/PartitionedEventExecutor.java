package com.mtx.trade.pipeline.event.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * 按稳定业务键路由到固定单线程分区的事件执行器。
 *
 * <p>同一业务键严格按提交顺序执行，不同业务键可以并行。每个分区使用有界队列；队列满时提交线程
 * 阻塞等待，从 Redis 读取端形成背压，不丢弃已经进入 PEL 的消息。</p>
 */
public final class PartitionedEventExecutor {

    private static final int RATE_WINDOW_SECONDS = 60;

    private final List<ThreadPoolExecutor> partitions;
    private final int queueCapacityPerWorker;
    private final long startedEpochSecond = System.currentTimeMillis() / 1000;
    private final LongAdder submitted = new LongAdder();
    private final LongAdder completed = new LongAdder();
    private final LongAdder backpressureCount = new LongAdder();
    private final long[] submissionEpochSeconds = new long[RATE_WINDOW_SECONDS];
    private final long[] submissionCounts = new long[RATE_WINDOW_SECONDS];
    private final long[] completionEpochSeconds = new long[RATE_WINDOW_SECONDS];
    private final long[] completionCounts = new long[RATE_WINDOW_SECONDS];
    private final long[] completionTotalNanos = new long[RATE_WINDOW_SECONDS];
    private final long[] completionMaxNanos = new long[RATE_WINDOW_SECONDS];

    public PartitionedEventExecutor(
            int workerCount,
            int queueCapacityPerWorker,
            String threadNamePrefix) {
        if (workerCount <= 0 || workerCount > 32) {
            throw new IllegalArgumentException("workerCount 必须为1~32");
        }
        if (queueCapacityPerWorker <= 0 || queueCapacityPerWorker > 100_000) {
            throw new IllegalArgumentException("queueCapacityPerWorker 必须为1~100000");
        }
        if (threadNamePrefix == null || threadNamePrefix.isBlank()) {
            throw new IllegalArgumentException("threadNamePrefix不能为空");
        }
        this.queueCapacityPerWorker = queueCapacityPerWorker;
        this.partitions = new ArrayList<>(workerCount);
        AtomicInteger threadSequence = new AtomicInteger();
        ThreadFactory threadFactory = task -> {
            Thread thread = new Thread(task, threadNamePrefix + threadSequence.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        };
        for (int index = 0; index < workerCount; index++) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    1,
                    1,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(queueCapacityPerWorker),
                    threadFactory,
                    this::blockUntilQueued);
            executor.prestartAllCoreThreads();
            partitions.add(executor);
        }
    }

    public CompletableFuture<Void> submit(String partitionKey, Runnable task) {
        Objects.requireNonNull(partitionKey, "partitionKey");
        Objects.requireNonNull(task, "task");
        CompletableFuture<Void> completion = new CompletableFuture<>();
        Runnable measuredTask = () -> {
            long startedNanos = System.nanoTime();
            try {
                task.run();
                markCompleted(System.nanoTime() - startedNanos);
                completion.complete(null);
            } catch (RuntimeException failure) {
                markCompleted(System.nanoTime() - startedNanos);
                completion.completeExceptionally(failure);
            } catch (Error failure) {
                markCompleted(System.nanoTime() - startedNanos);
                completion.completeExceptionally(failure);
                throw failure;
            }
        };
        submitted.increment();
        markSubmitted();
        try {
            partition(partitionKey).execute(measuredTask);
        } catch (RuntimeException failure) {
            completion.completeExceptionally(failure);
            throw failure;
        }
        return completion;
    }

    public Snapshot snapshot() {
        int activeWorkers = 0;
        int queuedTasks = 0;
        for (ThreadPoolExecutor partition : partitions) {
            activeWorkers += partition.getActiveCount();
            queuedTasks += partition.getQueue().size();
        }
        RateWindow submissionWindow = submittedWithinLastMinute();
        DurationWindow completionWindow = completedWithinLastMinute();
        long observedSeconds = Math.min(
                RATE_WINDOW_SECONDS,
                Math.max(1, System.currentTimeMillis() / 1000 - startedEpochSecond + 1));
        int totalQueueCapacity = partitions.size() * queueCapacityPerWorker;
        return new Snapshot(
                partitions.size(),
                activeWorkers,
                queuedTasks,
                totalQueueCapacity,
                percent(queuedTasks, totalQueueCapacity),
                submitted.sum(),
                completed.sum(),
                submissionWindow.count(),
                submissionWindow.count() / (double) observedSeconds,
                completionWindow.count(),
                completionWindow.count() / (double) observedSeconds,
                completionWindow.averageMillis(),
                completionWindow.maxMillis(),
                backpressureCount.sum());
    }

    public void shutdown() {
        partitions.forEach(ThreadPoolExecutor::shutdown);
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        boolean interrupted = false;
        for (ThreadPoolExecutor partition : partitions) {
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                partition.shutdownNow();
                continue;
            }
            try {
                if (!partition.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                    partition.shutdownNow();
                }
            } catch (InterruptedException e) {
                interrupted = true;
                partition.shutdownNow();
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private ThreadPoolExecutor partition(String partitionKey) {
        return partitions.get(Math.floorMod(partitionKey.hashCode(), partitions.size()));
    }

    private void blockUntilQueued(Runnable task, ThreadPoolExecutor executor) {
        if (executor.isShutdown()) {
            throw new RejectedExecutionException("事件执行器正在关闭");
        }
        backpressureCount.increment();
        try {
            while (!executor.isShutdown()) {
                if (executor.getQueue().offer(task, 100, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException("等待事件队列空间时被中断", e);
        }
        throw new RejectedExecutionException("事件执行器正在关闭");
    }

    private synchronized void markSubmitted() {
        long epochSecond = System.currentTimeMillis() / 1000;
        int index = (int) Math.floorMod(epochSecond, RATE_WINDOW_SECONDS);
        if (submissionEpochSeconds[index] != epochSecond) {
            submissionEpochSeconds[index] = epochSecond;
            submissionCounts[index] = 0;
        }
        submissionCounts[index]++;
    }

    private synchronized void markCompleted(long durationNanos) {
        completed.increment();
        long epochSecond = System.currentTimeMillis() / 1000;
        int index = (int) Math.floorMod(epochSecond, RATE_WINDOW_SECONDS);
        if (completionEpochSeconds[index] != epochSecond) {
            completionEpochSeconds[index] = epochSecond;
            completionCounts[index] = 0;
            completionTotalNanos[index] = 0;
            completionMaxNanos[index] = 0;
        }
        completionCounts[index]++;
        completionTotalNanos[index] += durationNanos;
        completionMaxNanos[index] = Math.max(completionMaxNanos[index], durationNanos);
    }

    private synchronized RateWindow submittedWithinLastMinute() {
        long now = System.currentTimeMillis() / 1000;
        long count = 0;
        for (int index = 0; index < RATE_WINDOW_SECONDS; index++) {
            long age = now - submissionEpochSeconds[index];
            if (age >= 0 && age < RATE_WINDOW_SECONDS) {
                count += submissionCounts[index];
            }
        }
        return new RateWindow(count);
    }

    private synchronized DurationWindow completedWithinLastMinute() {
        long now = System.currentTimeMillis() / 1000;
        long count = 0;
        long totalNanos = 0;
        long maxNanos = 0;
        for (int index = 0; index < RATE_WINDOW_SECONDS; index++) {
            long age = now - completionEpochSeconds[index];
            if (age >= 0 && age < RATE_WINDOW_SECONDS) {
                count += completionCounts[index];
                totalNanos += completionTotalNanos[index];
                maxNanos = Math.max(maxNanos, completionMaxNanos[index]);
            }
        }
        return new DurationWindow(count, totalNanos, maxNanos);
    }

    private static double percent(long value, long maximum) {
        return maximum <= 0 ? 0D : value * 100D / maximum;
    }

    public record Snapshot(
            int configuredWorkers,
            int activeWorkers,
            int queuedTasks,
            int queueCapacity,
            double queueUsagePercent,
            long submittedTasks,
            long completedTasks,
            long submittedLastMinute,
            double inputPerSecondLastMinute,
            long completedLastMinute,
            double throughputPerSecondLastMinute,
            double averageTaskDurationMillisLastMinute,
            double maxTaskDurationMillisLastMinute,
            long backpressureCount) {
    }

    private record RateWindow(long count) {
    }

    private record DurationWindow(long count, long totalNanos, long maxNanos) {

        private double averageMillis() {
            return count == 0 ? 0D : totalNanos / (double) count / 1_000_000D;
        }

        private double maxMillis() {
            return maxNanos / 1_000_000D;
        }
    }
}
