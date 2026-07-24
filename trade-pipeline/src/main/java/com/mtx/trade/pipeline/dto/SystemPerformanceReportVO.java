package com.mtx.trade.pipeline.dto;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

/** 可直接交给 AI 或运维人员分析的自包含性能诊断报告。 */
public record SystemPerformanceReportVO(
        String schemaVersion,
        AnalysisContext analysisContext,
        CapacityTargets capacityTargets,
        SystemPerformanceVO current,
        RecentWindowSummary recentWindow,
        List<PerformanceSample> recentSamples,
        Assessment assessment,
        Map<String, String> metricGuide) {

    public record AnalysisContext(
            String service,
            String purpose,
            String eventFlow,
            String concurrencyModel,
            String orderingGuarantee,
            String backpressureModel,
            int sampleIntervalSeconds,
            int retainedSampleCount,
            int historyWindowSeconds,
            String instructionsForAi) {
    }

    public record CapacityTargets(double orderRps, double paymentRps) {
    }

    public record PerformanceSample(
            OffsetDateTime sampledAt,
            ChannelSample order,
            ChannelSample payment,
            Double processCpuPercent,
            double heapUsagePercent,
            DataSourceSample pipelineDataSource,
            DataSourceSample storageDataSource) {
    }

    public record ChannelSample(
            double inputRpsLastMinute,
            double outputRpsLastMinute,
            double averageTaskDurationMillisLastMinute,
            double maxTaskDurationMillisLastMinute,
            int activeWorkers,
            int configuredWorkers,
            double queueUsagePercent,
            long backpressureCount,
            long successfulEvents,
            long failedEvents,
            long ingressAckFailures,
            long redisXackFailures,
            long streamLag,
            long pendingCount) {
    }

    public record DataSourceSample(
            double activeUsagePercent,
            int activeConnections,
            int configuredMaxPoolSize,
            int waitingThreads) {
    }

    public record RecentWindowSummary(
            int sampleCount,
            long observedSeconds,
            boolean trafficObserved,
            ChannelWindow order,
            ChannelWindow payment,
            ResourceWindow resources) {
    }

    public record ChannelWindow(
            double averageInputRps,
            double maxInputRps,
            double averageOutputRps,
            double maxOutputRps,
            double maxTaskDurationMillis,
            int maxActiveWorkers,
            double maxQueueUsagePercent,
            long backpressureIncrease,
            long successfulEventIncrease,
            long failedEventIncrease,
            long ingressAckFailureIncrease,
            long redisXackFailureIncrease,
            long maxStreamLag,
            long maxPendingCount) {
    }

    public record ResourceWindow(
            double averageProcessCpuPercent,
            double maxProcessCpuPercent,
            double maxHeapUsagePercent,
            double maxPipelineDataSourceUsagePercent,
            int maxPipelineWaitingThreads,
            double maxStorageDataSourceUsagePercent,
            int maxStorageWaitingThreads) {
    }

    public record Assessment(
            String status,
            boolean capacityAssessmentAvailable,
            String conclusion,
            List<String> evidence,
            List<String> recommendations) {
    }
}
