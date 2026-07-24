package com.mtx.trade.pipeline.service;

import com.mtx.trade.pipeline.config.OrderEventConsumerProperties;
import com.mtx.trade.pipeline.config.PerformanceMonitoringProperties;
import com.mtx.trade.pipeline.config.PaymentEventConsumerProperties;
import com.mtx.trade.pipeline.dto.SystemPerformanceReportVO;
import com.mtx.trade.pipeline.dto.SystemPerformanceVO;
import com.mtx.trade.pipeline.event.consumer.PartitionedEventExecutor;
import com.sun.management.OperatingSystemMXBean;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.stream.StreamInfo;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.time.OffsetDateTime;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.mtx.trade.pipeline.config.EventConsumerConfiguration.ORDER_EVENT_WORKER_EXECUTOR;
import static com.mtx.trade.pipeline.config.EventConsumerConfiguration.PAYMENT_EVENT_WORKER_EXECUTOR;
import static com.mtx.trade.pipeline.config.EventConsumerConfiguration.UNACKED_PULL_WORKER_EXECUTOR;

/** 采集不会扫描业务表的 Pipeline 实时性能快照。 */
@Service
public class SystemPerformanceService {

    private final StringRedisTemplate redisTemplate;
    private final OrderEventConsumerProperties orderProperties;
    private final PaymentEventConsumerProperties paymentProperties;
    private final ObjectProvider<PartitionedEventExecutor> orderWorkerProvider;
    private final ObjectProvider<PartitionedEventExecutor> paymentWorkerProvider;
    private final ThreadPoolTaskExecutor unackedPullExecutor;
    private final HikariDataSource pipelineDataSource;
    private final ObjectProvider<HikariDataSource> storageDataSourceProvider;
    private final PerformanceMonitoringProperties monitoringProperties;
    private final EventProcessingTelemetry telemetry;
    private final Deque<SystemPerformanceReportVO.PerformanceSample> recentSamples = new ArrayDeque<>();

    public SystemPerformanceService(
            StringRedisTemplate redisTemplate,
            OrderEventConsumerProperties orderProperties,
            PaymentEventConsumerProperties paymentProperties,
            @Qualifier(ORDER_EVENT_WORKER_EXECUTOR)
            ObjectProvider<PartitionedEventExecutor> orderWorkerProvider,
            @Qualifier(PAYMENT_EVENT_WORKER_EXECUTOR)
            ObjectProvider<PartitionedEventExecutor> paymentWorkerProvider,
            @Qualifier(UNACKED_PULL_WORKER_EXECUTOR) ThreadPoolTaskExecutor unackedPullExecutor,
            @Qualifier("pipelineActualDataSource") HikariDataSource pipelineDataSource,
            @Qualifier("storageActualDataSource")
            ObjectProvider<HikariDataSource> storageDataSourceProvider,
            PerformanceMonitoringProperties monitoringProperties,
            EventProcessingTelemetry telemetry) {
        this.redisTemplate = redisTemplate;
        this.orderProperties = orderProperties;
        this.paymentProperties = paymentProperties;
        this.orderWorkerProvider = orderWorkerProvider;
        this.paymentWorkerProvider = paymentWorkerProvider;
        this.unackedPullExecutor = unackedPullExecutor;
        this.pipelineDataSource = pipelineDataSource;
        this.storageDataSourceProvider = storageDataSourceProvider;
        this.monitoringProperties = monitoringProperties;
        this.telemetry = telemetry;
    }

    public SystemPerformanceReportVO current() {
        SystemPerformanceVO current = snapshot();
        List<SystemPerformanceReportVO.PerformanceSample> samples = historyWithCurrent(toSample(current));
        SystemPerformanceReportVO.RecentWindowSummary recentWindow = summarize(samples);
        return new SystemPerformanceReportVO(
                "trade-pipeline-performance/v2",
                analysisContext(),
                new SystemPerformanceReportVO.CapacityTargets(
                        monitoringProperties.getOrderTargetRps(),
                        monitoringProperties.getPaymentTargetRps()),
                current,
                recentWindow,
                samples,
                assess(recentWindow),
                metricGuide());
    }

    /** 由后台采样任务调用，保留紧凑的最近窗口，不查询任何业务表。 */
    public void sample() {
        SystemPerformanceReportVO.PerformanceSample sample = toSample(snapshot());
        synchronized (recentSamples) {
            recentSamples.addLast(sample);
            int historySize = validatedHistorySize();
            while (recentSamples.size() > historySize) {
                recentSamples.removeFirst();
            }
        }
    }

    private SystemPerformanceVO snapshot() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();

        Map<String, SystemPerformanceVO.EventWorkerMetrics> eventWorkers = new LinkedHashMap<>();
        eventWorkers.put("order", eventWorker(
                orderWorkerProvider.getIfAvailable(),
                telemetry.snapshot(EventProcessingTelemetry.ORDER)));
        eventWorkers.put("payment", eventWorker(
                paymentWorkerProvider.getIfAvailable(),
                telemetry.snapshot(EventProcessingTelemetry.PAYMENT)));

        Map<String, SystemPerformanceVO.DataSourceMetrics> dataSources = new LinkedHashMap<>();
        dataSources.put("pipeline", dataSource(pipelineDataSource));
        dataSources.put("storage", dataSource(storageDataSourceProvider.getIfAvailable()));

        Map<String, SystemPerformanceVO.RedisStreamMetrics> streams = new LinkedHashMap<>();
        streams.put("order", stream(orderProperties.getStreamKey(), orderProperties.getGroup()));
        streams.put("payment", stream(paymentProperties.getStreamKey(), paymentProperties.getGroup()));

        return new SystemPerformanceVO(
                OffsetDateTime.now(),
                runtime.getUptime() / 1000,
                cpu(),
                memory(memory),
                new SystemPerformanceVO.ThreadMetrics(
                        threads.getThreadCount(),
                        threads.getPeakThreadCount(),
                        threads.getDaemonThreadCount(),
                        threads.getTotalStartedThreadCount()),
                gc(),
                eventWorkers,
                taskPool(unackedPullExecutor),
                dataSources,
                streams);
    }

    private SystemPerformanceReportVO.AnalysisContext analysisContext() {
        int intervalSeconds = validatedSampleIntervalSeconds();
        int historySize = validatedHistorySize();
        return new SystemPerformanceReportVO.AnalysisContext(
                "trade-pipeline",
                "Evaluate whether current worker and connection-pool settings can sustain the target event rate.",
                "Ingress MySQL event -> Redis Stream -> Pipeline Storage read -> parse -> business transaction "
                        + "-> processing audit -> Ingress HTTP ACK -> Redis XACK/XDEL",
                "One Redis polling thread dispatches order events to %d keyed workers and payment events to %d "
                        .formatted(orderProperties.getWorkerCount(), paymentProperties.getWorkerCount())
                        + "keyed workers.",
                "sourceSystem + eventKey is always routed to the same single-thread partition; the same business "
                        + "key stays ordered while different keys run concurrently.",
                "Each worker partition has a bounded queue. A full partition blocks Redis dispatch, keeps records "
                        + "recoverable in PEL and is reported by backpressureCount.",
                intervalSeconds,
                historySize,
                intervalSeconds * historySize,
                "Use current plus recentWindow and recentSamples. Decide whether the target RPS was actually "
                        + "applied, then identify worker, CPU, Pipeline DB, Storage DB or Redis lag saturation. "
                        + "Do not infer capacity from an IDLE or INSUFFICIENT_LOAD report.");
    }

    private List<SystemPerformanceReportVO.PerformanceSample> historyWithCurrent(
            SystemPerformanceReportVO.PerformanceSample current) {
        List<SystemPerformanceReportVO.PerformanceSample> samples;
        synchronized (recentSamples) {
            samples = new ArrayList<>(recentSamples);
        }
        if (samples.isEmpty()
                || Duration.between(samples.get(samples.size() - 1).sampledAt(), current.sampledAt())
                .abs().toMillis() >= 1_000) {
            samples.add(current);
        }
        while (samples.size() > validatedHistorySize()) {
            samples.remove(0);
        }
        return List.copyOf(samples);
    }

    private static SystemPerformanceReportVO.PerformanceSample toSample(SystemPerformanceVO snapshot) {
        return new SystemPerformanceReportVO.PerformanceSample(
                snapshot.sampledAt(),
                channelSample(snapshot, "order"),
                channelSample(snapshot, "payment"),
                snapshot.cpu().processUsagePercent(),
                snapshot.memory().heapUsagePercent(),
                dataSourceSample(snapshot.dataSources().get("pipeline")),
                dataSourceSample(snapshot.dataSources().get("storage")));
    }

    private static SystemPerformanceReportVO.ChannelSample channelSample(
            SystemPerformanceVO snapshot, String channel) {
        SystemPerformanceVO.EventWorkerMetrics worker = snapshot.eventWorkers().get(channel);
        SystemPerformanceVO.RedisStreamMetrics stream = snapshot.streams().get(channel);
        if (worker == null) {
            return new SystemPerformanceReportVO.ChannelSample(
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1);
        }
        long lag = stream == null || !stream.available() || stream.lag() == null ? -1 : stream.lag();
        long pending = stream == null || !stream.available() ? -1 : stream.pendingCount();
        return new SystemPerformanceReportVO.ChannelSample(
                worker.inputPerSecondLastMinute(),
                worker.throughputPerSecondLastMinute(),
                worker.averageTaskDurationMillisLastMinute(),
                worker.maxTaskDurationMillisLastMinute(),
                worker.activeWorkers(),
                worker.configuredWorkers(),
                worker.queueUsagePercent(),
                worker.backpressureCount(),
                worker.successfulEvents(),
                worker.failedEvents(),
                worker.ingressAckFailures(),
                worker.redisXackFailures(),
                lag,
                pending);
    }

    private static SystemPerformanceReportVO.DataSourceSample dataSourceSample(
            SystemPerformanceVO.DataSourceMetrics dataSource) {
        if (dataSource == null || !dataSource.available()) {
            return new SystemPerformanceReportVO.DataSourceSample(0, 0, 0, 0);
        }
        return new SystemPerformanceReportVO.DataSourceSample(
                percent(dataSource.activeConnections(), dataSource.configuredMaxPoolSize()),
                dataSource.activeConnections(),
                dataSource.configuredMaxPoolSize(),
                dataSource.waitingThreads());
    }

    static SystemPerformanceReportVO.RecentWindowSummary summarize(
            List<SystemPerformanceReportVO.PerformanceSample> samples) {
        if (samples.isEmpty()) {
            return new SystemPerformanceReportVO.RecentWindowSummary(
                    0, 0, false, emptyChannelWindow(), emptyChannelWindow(), emptyResourceWindow());
        }
        long observedSeconds = samples.size() <= 1 ? 0 : Math.max(
                0,
                Duration.between(samples.get(0).sampledAt(), samples.get(samples.size() - 1).sampledAt())
                        .getSeconds());
        SystemPerformanceReportVO.ChannelWindow order = summarizeChannel(
                samples.stream().map(SystemPerformanceReportVO.PerformanceSample::order).toList());
        SystemPerformanceReportVO.ChannelWindow payment = summarizeChannel(
                samples.stream().map(SystemPerformanceReportVO.PerformanceSample::payment).toList());
        boolean trafficObserved = hasTraffic(order) || hasTraffic(payment);
        return new SystemPerformanceReportVO.RecentWindowSummary(
                samples.size(),
                observedSeconds,
                trafficObserved,
                order,
                payment,
                summarizeResources(samples));
    }

    private static SystemPerformanceReportVO.ChannelWindow summarizeChannel(
            List<SystemPerformanceReportVO.ChannelSample> samples) {
        if (samples.isEmpty()) {
            return emptyChannelWindow();
        }
        double inputTotal = 0;
        double outputTotal = 0;
        double maxInput = 0;
        double maxOutput = 0;
        double maxDuration = 0;
        int maxActive = 0;
        double maxQueue = 0;
        long minBackpressure = Long.MAX_VALUE;
        long maxBackpressure = 0;
        long minSuccess = Long.MAX_VALUE;
        long maxSuccess = 0;
        long minFailure = Long.MAX_VALUE;
        long maxFailure = 0;
        long minIngressAckFailure = Long.MAX_VALUE;
        long maxIngressAckFailure = 0;
        long minRedisXackFailure = Long.MAX_VALUE;
        long maxRedisXackFailure = 0;
        long maxLag = 0;
        long maxPending = 0;
        for (SystemPerformanceReportVO.ChannelSample sample : samples) {
            inputTotal += sample.inputRpsLastMinute();
            outputTotal += sample.outputRpsLastMinute();
            maxInput = Math.max(maxInput, sample.inputRpsLastMinute());
            maxOutput = Math.max(maxOutput, sample.outputRpsLastMinute());
            maxDuration = Math.max(maxDuration, sample.maxTaskDurationMillisLastMinute());
            maxActive = Math.max(maxActive, sample.activeWorkers());
            maxQueue = Math.max(maxQueue, sample.queueUsagePercent());
            minBackpressure = Math.min(minBackpressure, sample.backpressureCount());
            maxBackpressure = Math.max(maxBackpressure, sample.backpressureCount());
            minSuccess = Math.min(minSuccess, sample.successfulEvents());
            maxSuccess = Math.max(maxSuccess, sample.successfulEvents());
            minFailure = Math.min(minFailure, sample.failedEvents());
            maxFailure = Math.max(maxFailure, sample.failedEvents());
            minIngressAckFailure = Math.min(minIngressAckFailure, sample.ingressAckFailures());
            maxIngressAckFailure = Math.max(maxIngressAckFailure, sample.ingressAckFailures());
            minRedisXackFailure = Math.min(minRedisXackFailure, sample.redisXackFailures());
            maxRedisXackFailure = Math.max(maxRedisXackFailure, sample.redisXackFailures());
            if (sample.streamLag() >= 0) {
                maxLag = Math.max(maxLag, sample.streamLag());
            }
            if (sample.pendingCount() >= 0) {
                maxPending = Math.max(maxPending, sample.pendingCount());
            }
        }
        return new SystemPerformanceReportVO.ChannelWindow(
                inputTotal / samples.size(),
                maxInput,
                outputTotal / samples.size(),
                maxOutput,
                maxDuration,
                maxActive,
                maxQueue,
                minBackpressure == Long.MAX_VALUE ? 0 : maxBackpressure - minBackpressure,
                increase(minSuccess, maxSuccess),
                increase(minFailure, maxFailure),
                increase(minIngressAckFailure, maxIngressAckFailure),
                increase(minRedisXackFailure, maxRedisXackFailure),
                maxLag,
                maxPending);
    }

    private static SystemPerformanceReportVO.ResourceWindow summarizeResources(
            List<SystemPerformanceReportVO.PerformanceSample> samples) {
        double cpuTotal = 0;
        int cpuSamples = 0;
        double maxCpu = 0;
        double maxHeap = 0;
        double maxPipelineUsage = 0;
        int maxPipelineWaiting = 0;
        double maxStorageUsage = 0;
        int maxStorageWaiting = 0;
        for (SystemPerformanceReportVO.PerformanceSample sample : samples) {
            if (sample.processCpuPercent() != null) {
                cpuTotal += sample.processCpuPercent();
                cpuSamples++;
                maxCpu = Math.max(maxCpu, sample.processCpuPercent());
            }
            maxHeap = Math.max(maxHeap, sample.heapUsagePercent());
            maxPipelineUsage = Math.max(
                    maxPipelineUsage, sample.pipelineDataSource().activeUsagePercent());
            maxPipelineWaiting = Math.max(
                    maxPipelineWaiting, sample.pipelineDataSource().waitingThreads());
            maxStorageUsage = Math.max(
                    maxStorageUsage, sample.storageDataSource().activeUsagePercent());
            maxStorageWaiting = Math.max(
                    maxStorageWaiting, sample.storageDataSource().waitingThreads());
        }
        return new SystemPerformanceReportVO.ResourceWindow(
                cpuSamples == 0 ? 0 : cpuTotal / cpuSamples,
                maxCpu,
                maxHeap,
                maxPipelineUsage,
                maxPipelineWaiting,
                maxStorageUsage,
                maxStorageWaiting);
    }

    SystemPerformanceReportVO.Assessment assess(
            SystemPerformanceReportVO.RecentWindowSummary window) {
        List<String> evidence = new ArrayList<>();
        List<String> recommendations = new ArrayList<>();
        if (!window.trafficObserved()) {
            evidence.add("No order or payment input/output, lag, pending or backpressure was observed.");
            recommendations.add("Run the target replay while the background sampler collects at least 12 samples "
                    + "(about one minute), then request this endpoint once.");
            return new SystemPerformanceReportVO.Assessment(
                    "IDLE",
                    false,
                    "The service is healthy at idle, but this report cannot validate throughput capacity.",
                    evidence,
                    recommendations);
        }

        boolean useOrder = hasTraffic(window.order());
        String channelName = useOrder ? "order" : "payment";
        SystemPerformanceReportVO.ChannelWindow channel = useOrder ? window.order() : window.payment();
        double targetRps = useOrder
                ? monitoringProperties.getOrderTargetRps()
                : monitoringProperties.getPaymentTargetRps();
        SystemPerformanceReportVO.ResourceWindow resources = window.resources();
        boolean targetLoadApplied = channel.maxInputRps() >= targetRps * 0.8D;
        long significantLagThreshold = Math.max(10L, (long) Math.ceil(targetRps));

        evidence.add("%s max input/output RPS: %.2f / %.2f; configured target: %.2f."
                .formatted(channelName, channel.maxInputRps(), channel.maxOutputRps(), targetRps));
        evidence.add("%s max workers/queue/lag/pending: %d / %.2f%% / %d / %d."
                .formatted(
                        channelName,
                        channel.maxActiveWorkers(),
                        channel.maxQueueUsagePercent(),
                        channel.maxStreamLag(),
                        channel.maxPendingCount()));
        evidence.add(("%s interval success/processing-failure/Ingress-ACK-failure/Redis-XACK-failure: "
                        + "%d / %d / %d / %d.")
                .formatted(
                        channelName,
                        channel.successfulEventIncrease(),
                        channel.failedEventIncrease(),
                        channel.ingressAckFailureIncrease(),
                        channel.redisXackFailureIncrease()));
        evidence.add("Max process CPU/Pipeline DB/Storage DB utilization: %.2f%% / %.2f%% / %.2f%%."
                .formatted(
                        resources.maxProcessCpuPercent(),
                        resources.maxPipelineDataSourceUsagePercent(),
                        resources.maxStorageDataSourceUsagePercent()));

        if (channel.failedEventIncrease() > 0) {
            recommendations.add("Inspect Pipeline processing-audit failureStage/errorCode and application logs "
                    + "before increasing concurrency; failed events can make raw completion RPS misleading.");
            return assessment(
                    "PROCESSING_FAILURES",
                    true,
                    "Business processing failures occurred inside the observation window.",
                    evidence,
                    recommendations);
        }
        if (channel.ingressAckFailureIncrease() > 0 || channel.redisXackFailureIncrease() > 0) {
            recommendations.add("Inspect Ingress HTTP ACK latency/connectivity and Redis XACK errors. Business "
                    + "persistence may already be complete, but acknowledgement recovery is adding duplicate work.");
            return assessment(
                    "ACK_FAILURES",
                    true,
                    "Ingress ACK or Redis XACK failures occurred inside the observation window.",
                    evidence,
                    recommendations);
        }
        if (resources.maxPipelineWaitingThreads() > 0
                || resources.maxPipelineDataSourceUsagePercent()
                >= monitoringProperties.getDataSourceWarningPercent()) {
            recommendations.add("Do not increase event workers first. Investigate Pipeline SQL latency, locks and "
                    + "connection-pool sizing because the Pipeline datasource is saturated.");
            return assessment(
                    "PIPELINE_DB_BOUND",
                    targetLoadApplied,
                    "Pipeline database capacity is the dominant bottleneck signal.",
                    evidence,
                    recommendations);
        }
        if (resources.maxStorageWaitingThreads() > 0
                || resources.maxStorageDataSourceUsagePercent()
                >= monitoringProperties.getDataSourceWarningPercent()) {
            recommendations.add("Do not increase event workers first. Increase Storage read capacity or reduce "
                    + "Storage query latency because workers are waiting for Storage connections.");
            return assessment(
                    "STORAGE_DB_BOUND",
                    targetLoadApplied,
                    "Storage read capacity is the dominant bottleneck signal.",
                    evidence,
                    recommendations);
        }
        if (resources.maxProcessCpuPercent() >= monitoringProperties.getCpuWarningPercent()) {
            recommendations.add("Keep worker count unchanged or scale the process horizontally; additional local "
                    + "workers are unlikely to improve throughput while CPU is saturated.");
            return assessment(
                    "CPU_BOUND",
                    targetLoadApplied,
                    "Process CPU is the dominant bottleneck signal.",
                    evidence,
                    recommendations);
        }
        if (channel.backpressureIncrease() > 0
                || channel.maxQueueUsagePercent() >= monitoringProperties.getQueueWarningPercent()) {
            recommendations.add("CPU and datasource headroom permitting, raise the relevant worker count in small "
                    + "steps (for example +2) and repeat the same replay.");
            return assessment(
                    "WORKER_BACKPRESSURE",
                    targetLoadApplied,
                    "The keyed worker queue cannot drain as fast as Redis dispatch.",
                    evidence,
                    recommendations);
        }
        if (channel.maxStreamLag() >= significantLagThreshold) {
            recommendations.add("Compare input and output RPS. If CPU and datasource utilization remain low, add "
                    + "workers; otherwise address the saturated resource shown in recentSamples.");
            return assessment(
                    "STREAM_LAGGING",
                    targetLoadApplied,
                    "Redis Stream lag exceeded roughly one second of target traffic.",
                    evidence,
                    recommendations);
        }
        if (!targetLoadApplied) {
            recommendations.add("Increase replay-side effective RPS/concurrency until input RPS reaches at least "
                    + "80% of the configured target; the current workload is too low to prove capacity.");
            return assessment(
                    "INSUFFICIENT_LOAD",
                    false,
                    "The observed workload did not reach the configured target, so capacity is not proven.",
                    evidence,
                    recommendations);
        }
        if (channel.maxOutputRps() >= targetRps * 0.9D) {
            recommendations.add("Keep the current configuration and extend the replay duration to verify that lag "
                    + "and queue usage stay bounded over time.");
            return assessment(
                    "TARGET_MET",
                    true,
                    "Observed output reached the target range without saturation signals.",
                    evidence,
                    recommendations);
        }
        recommendations.add("The target load was applied but output remained below target without an obvious "
                + "resource saturation signal. Inspect per-event task duration and external ACK latency.");
        return assessment(
                "UNDER_TARGET",
                true,
                "Observed output stayed below the configured target.",
                evidence,
                recommendations);
    }

    private static SystemPerformanceReportVO.Assessment assessment(
            String status,
            boolean available,
            String conclusion,
            List<String> evidence,
            List<String> recommendations) {
        return new SystemPerformanceReportVO.Assessment(
                status, available, conclusion, List.copyOf(evidence), List.copyOf(recommendations));
    }

    private static boolean hasTraffic(SystemPerformanceReportVO.ChannelWindow channel) {
        return channel.maxInputRps() > 0
                || channel.maxOutputRps() > 0
                || channel.maxStreamLag() > 0
                || channel.maxPendingCount() > 0
                || channel.backpressureIncrease() > 0
                || channel.successfulEventIncrease() > 0
                || channel.failedEventIncrease() > 0;
    }

    private static SystemPerformanceReportVO.ChannelWindow emptyChannelWindow() {
        return new SystemPerformanceReportVO.ChannelWindow(
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static SystemPerformanceReportVO.ResourceWindow emptyResourceWindow() {
        return new SystemPerformanceReportVO.ResourceWindow(0, 0, 0, 0, 0, 0, 0);
    }

    private static Map<String, String> metricGuide() {
        Map<String, String> guide = new LinkedHashMap<>();
        guide.put("inputRpsLastMinute", "Rate accepted by keyed workers; compare it with replay-side successful RPS.");
        guide.put("outputRpsLastMinute", "Rate of completed event tasks in the rolling window.");
        guide.put("averageTaskDurationMillisLastMinute", "Mean end-to-end worker task time in the rolling window.");
        guide.put("queueUsagePercent", "Bounded keyed-worker queue usage; sustained growth means workers are behind.");
        guide.put("backpressureCount", "Cumulative times a full partition queue blocked Redis dispatch.");
        guide.put("failedEvents", "Cumulative real-time events that reached the durable processing-failure path.");
        guide.put("ingressAckFailures", "Business processing succeeded but Ingress HTTP ACK failed.");
        guide.put("redisXackFailures", "Redis delivery could not be acknowledged and remains recoverable in PEL.");
        guide.put("streamLag", "Messages not yet delivered to the consumer group; growth means Redis input is ahead.");
        guide.put("pendingCount", "Delivered but not yet XACKed messages, including normal in-flight tasks; a value "
                + "near activeWorkers + queuedTasks is not Stream backlog by itself.");
        guide.put("waitingThreads", "Threads waiting for a Hikari connection; any sustained value above zero is a bottleneck.");
        guide.put("capacityAssessmentAvailable", "False means the observed load was insufficient to prove target capacity.");
        return Map.copyOf(guide);
    }

    private static long increase(long minimum, long maximum) {
        return minimum == Long.MAX_VALUE ? 0 : Math.max(0, maximum - minimum);
    }

    private int validatedHistorySize() {
        int historySize = monitoringProperties.getHistorySize();
        if (historySize < 2 || historySize > 720) {
            throw new IllegalArgumentException(
                    "trade.pipeline.performance-monitoring.history-size 必须为2~720");
        }
        return historySize;
    }

    private int validatedSampleIntervalSeconds() {
        long intervalMs = monitoringProperties.getSampleIntervalMs();
        if (intervalMs < 1_000 || intervalMs > 60_000) {
            throw new IllegalArgumentException(
                    "trade.pipeline.performance-monitoring.sample-interval-ms 必须为1000~60000");
        }
        return Math.toIntExact(Math.max(1, intervalMs / 1_000));
    }

    private static SystemPerformanceVO.CpuMetrics cpu() {
        java.lang.management.OperatingSystemMXBean base = ManagementFactory.getOperatingSystemMXBean();
        if (base instanceof OperatingSystemMXBean operatingSystem) {
            return new SystemPerformanceVO.CpuMetrics(
                    operatingSystem.getAvailableProcessors(),
                    usagePercent(operatingSystem.getProcessCpuLoad()),
                    usagePercent(operatingSystem.getCpuLoad()),
                    operatingSystem.getSystemLoadAverage(),
                    operatingSystem.getProcessCpuTime());
        }
        return new SystemPerformanceVO.CpuMetrics(
                base.getAvailableProcessors(), null, null, base.getSystemLoadAverage(), -1);
    }

    private static SystemPerformanceVO.MemoryMetrics memory(MemoryMXBean memory) {
        MemoryUsage heap = memory.getHeapMemoryUsage();
        MemoryUsage nonHeap = memory.getNonHeapMemoryUsage();
        return new SystemPerformanceVO.MemoryMetrics(
                heap.getUsed(),
                heap.getCommitted(),
                heap.getMax(),
                percent(heap.getUsed(), heap.getMax()),
                nonHeap.getUsed(),
                nonHeap.getCommitted());
    }

    private static SystemPerformanceVO.GcMetrics gc() {
        long count = 0;
        long timeMillis = 0;
        for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (collector.getCollectionCount() >= 0) {
                count += collector.getCollectionCount();
            }
            if (collector.getCollectionTime() >= 0) {
                timeMillis += collector.getCollectionTime();
            }
        }
        return new SystemPerformanceVO.GcMetrics(count, timeMillis);
    }

    private static SystemPerformanceVO.EventWorkerMetrics eventWorker(
            PartitionedEventExecutor executor,
            EventProcessingTelemetry.Snapshot telemetry) {
        if (executor == null) {
            return new SystemPerformanceVO.EventWorkerMetrics(
                    false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    telemetry.successfulEvents(),
                    telemetry.failedEvents(),
                    telemetry.ingressAckFailures(),
                    telemetry.redisXackFailures());
        }
        PartitionedEventExecutor.Snapshot snapshot = executor.snapshot();
        return new SystemPerformanceVO.EventWorkerMetrics(
                true,
                snapshot.configuredWorkers(),
                snapshot.activeWorkers(),
                snapshot.queuedTasks(),
                snapshot.queueCapacity(),
                snapshot.queueUsagePercent(),
                snapshot.submittedTasks(),
                snapshot.completedTasks(),
                snapshot.submittedLastMinute(),
                snapshot.inputPerSecondLastMinute(),
                snapshot.completedLastMinute(),
                snapshot.throughputPerSecondLastMinute(),
                snapshot.averageTaskDurationMillisLastMinute(),
                snapshot.maxTaskDurationMillisLastMinute(),
                snapshot.backpressureCount(),
                telemetry.successfulEvents(),
                telemetry.failedEvents(),
                telemetry.ingressAckFailures(),
                telemetry.redisXackFailures());
    }

    private static SystemPerformanceVO.TaskPoolMetrics taskPool(ThreadPoolTaskExecutor executor) {
        ThreadPoolTaskExecutor source = executor;
        java.util.concurrent.ThreadPoolExecutor pool = source.getThreadPoolExecutor();
        return new SystemPerformanceVO.TaskPoolMetrics(
                source.getCorePoolSize(),
                source.getMaxPoolSize(),
                source.getPoolSize(),
                source.getActiveCount(),
                pool.getQueue().size(),
                pool.getQueue().remainingCapacity(),
                pool.getCompletedTaskCount());
    }

    private static SystemPerformanceVO.DataSourceMetrics dataSource(HikariDataSource dataSource) {
        if (dataSource == null) {
            return new SystemPerformanceVO.DataSourceMetrics(false, 0, 0, 0, 0, 0);
        }
        HikariPoolMXBean pool = dataSource.getHikariPoolMXBean();
        if (pool == null) {
            return new SystemPerformanceVO.DataSourceMetrics(
                    false, dataSource.getMaximumPoolSize(), 0, 0, 0, 0);
        }
        return new SystemPerformanceVO.DataSourceMetrics(
                true,
                dataSource.getMaximumPoolSize(),
                pool.getTotalConnections(),
                pool.getActiveConnections(),
                pool.getIdleConnections(),
                pool.getThreadsAwaitingConnection());
    }

    private SystemPerformanceVO.RedisStreamMetrics stream(String streamKey, String groupName) {
        try {
            Long size = redisTemplate.opsForStream().size(streamKey);
            StreamInfo.XInfoGroup group = redisTemplate.opsForStream().groups(streamKey).stream()
                    .filter(candidate -> groupName.equals(candidate.groupName()))
                    .findFirst()
                    .orElse(null);
            if (group == null) {
                return new SystemPerformanceVO.RedisStreamMetrics(
                        false, streamKey, groupName, valueOrZero(size), 0, 0, null,
                        "consumer group not found");
            }
            return new SystemPerformanceVO.RedisStreamMetrics(
                    true,
                    streamKey,
                    groupName,
                    valueOrZero(size),
                    group.consumerCount(),
                    group.pendingCount(),
                    rawLong(group, "lag"),
                    null);
        } catch (RuntimeException failure) {
            return new SystemPerformanceVO.RedisStreamMetrics(
                    false, streamKey, groupName, 0, 0, 0, null,
                    failure.getClass().getSimpleName());
        }
    }

    private static Long rawLong(StreamInfo.XInfoGroup group, String key) {
        Object value = group.getRaw().get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String stringValue) {
            try {
                return Long.parseLong(stringValue);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private static long valueOrZero(Long value) {
        return value == null ? 0 : value;
    }

    private static Double usagePercent(double value) {
        return value < 0 ? null : value * 100D;
    }

    private static double percent(long value, long maximum) {
        return maximum <= 0 ? 0D : value * 100D / maximum;
    }
}
