package com.mtx.trade.pipeline.dto;

import java.time.OffsetDateTime;
import java.util.Map;

/** Pipeline 当前进程、消费器和基础设施的轻量性能快照。 */
public record SystemPerformanceVO(
        OffsetDateTime sampledAt,
        long uptimeSeconds,
        CpuMetrics cpu,
        MemoryMetrics memory,
        ThreadMetrics threads,
        GcMetrics gc,
        Map<String, EventWorkerMetrics> eventWorkers,
        TaskPoolMetrics unackedPullWorkers,
        Map<String, DataSourceMetrics> dataSources,
        Map<String, RedisStreamMetrics> streams) {

    public record CpuMetrics(
            int availableProcessors,
            Double processUsagePercent,
            Double systemUsagePercent,
            double systemLoadAverage,
            long processCpuTimeNanos) {
    }

    public record MemoryMetrics(
            long heapUsedBytes,
            long heapCommittedBytes,
            long heapMaxBytes,
            double heapUsagePercent,
            long nonHeapUsedBytes,
            long nonHeapCommittedBytes) {
    }

    public record ThreadMetrics(int live, int peak, int daemon, long startedTotal) {
    }

    public record GcMetrics(long collectionCount, long collectionTimeMillis) {
    }

    public record EventWorkerMetrics(
            boolean available,
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
            long backpressureCount,
            long successfulEvents,
            long failedEvents,
            long ingressAckFailures,
            long redisXackFailures) {
    }

    public record TaskPoolMetrics(
            int corePoolSize,
            int maxPoolSize,
            int currentPoolSize,
            int activeThreads,
            int queuedTasks,
            int remainingQueueCapacity,
            long completedTasks) {
    }

    public record DataSourceMetrics(
            boolean available,
            int configuredMaxPoolSize,
            int totalConnections,
            int activeConnections,
            int idleConnections,
            int waitingThreads) {
    }

    public record RedisStreamMetrics(
            boolean available,
            String streamKey,
            String group,
            long streamLength,
            long consumerCount,
            long pendingCount,
            Long lag,
            String error) {
    }
}
