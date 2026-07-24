package com.mtx.trade.pipeline.service;

import com.mtx.trade.pipeline.config.PerformanceMonitoringProperties;
import com.mtx.trade.pipeline.dto.SystemPerformanceReportVO;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SystemPerformanceServiceTest {

    @Test
    void shouldRefuseCapacityConclusionWhenAppliedLoadIsBelowTarget() {
        SystemPerformanceService service = service();
        var window = SystemPerformanceService.summarize(List.of(
                sample(60, 60, 0, 0, 0, 20, 0, 0),
                sample(60, 60, 0, 0, 10, 25, 0, 0)));

        var assessment = service.assess(window);

        assertThat(assessment.status()).isEqualTo("INSUFFICIENT_LOAD");
        assertThat(assessment.capacityAssessmentAvailable()).isFalse();
    }

    @Test
    void shouldTreatSmallLagAndWorkerSizedPendingAsNormalInFlightWork() {
        SystemPerformanceService service = service();
        var window = SystemPerformanceService.summarize(List.of(
                sample(56, 56, 0, 0, 0, 20, 0, 0),
                sampleWithRedisState(56, 56, 10, 25, 2, 8)));

        var assessment = service.assess(window);

        assertThat(assessment.status()).isEqualTo("INSUFFICIENT_LOAD");
        assertThat(assessment.capacityAssessmentAvailable()).isFalse();
    }

    @Test
    void shouldReportSignificantStreamLagWithoutTreatingPendingAsLag() {
        SystemPerformanceService service = service();
        var window = SystemPerformanceService.summarize(List.of(
                sample(100, 90, 0, 0, 0, 20, 0, 0),
                sampleWithRedisState(100, 90, 10, 25, 120, 8)));

        var assessment = service.assess(window);

        assertThat(assessment.status()).isEqualTo("STREAM_LAGGING");
        assertThat(assessment.capacityAssessmentAvailable()).isTrue();
    }

    @Test
    void shouldReportTargetMetWhenThroughputReachesTargetWithoutSaturation() {
        SystemPerformanceService service = service();
        var window = SystemPerformanceService.summarize(List.of(
                sample(100, 98, 0, 0, 0, 35, 0, 0),
                sample(105, 101, 0, 0, 10, 40, 0, 0)));

        var assessment = service.assess(window);

        assertThat(assessment.status()).isEqualTo("TARGET_MET");
        assertThat(assessment.capacityAssessmentAvailable()).isTrue();
    }

    @Test
    void shouldPrioritizePipelineDatasourceSaturation() {
        SystemPerformanceService service = service();
        var window = SystemPerformanceService.summarize(List.of(
                sample(100, 70, 0, 0, 0, 30, 95, 1),
                sample(100, 70, 0, 0, 10, 35, 100, 2)));

        var assessment = service.assess(window);

        assertThat(assessment.status()).isEqualTo("PIPELINE_DB_BOUND");
        assertThat(assessment.conclusion()).contains("database");
    }

    @Test
    void shouldExposeProcessingFailuresBeforeCapacityAdvice() {
        SystemPerformanceService service = service();
        var window = SystemPerformanceService.summarize(List.of(
                sample(100, 100, 10, 0, 0, 30, 20, 0),
                sample(100, 100, 12, 2, 10, 30, 20, 0)));

        var assessment = service.assess(window);

        assertThat(assessment.status()).isEqualTo("PROCESSING_FAILURES");
    }

    private static SystemPerformanceService service() {
        PerformanceMonitoringProperties properties = new PerformanceMonitoringProperties();
        properties.setOrderTargetRps(100);
        properties.setPaymentTargetRps(100);
        return new SystemPerformanceService(
                null, null, null, null, null, null, null, null, properties, null);
    }

    private static SystemPerformanceReportVO.PerformanceSample sample(
            double inputRps,
            double outputRps,
            long successfulEvents,
            long failedEvents,
            int seconds,
            double cpuPercent,
            double pipelineDataSourceUsage,
            int pipelineWaitingThreads) {
        var order = new SystemPerformanceReportVO.ChannelSample(
                inputRps,
                outputRps,
                10,
                20,
                8,
                8,
                0,
                0,
                successfulEvents,
                failedEvents,
                0,
                0,
                0,
                0);
        var payment = new SystemPerformanceReportVO.ChannelSample(
                0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0);
        return new SystemPerformanceReportVO.PerformanceSample(
                OffsetDateTime.now().plusSeconds(seconds),
                order,
                payment,
                cpuPercent,
                30,
                new SystemPerformanceReportVO.DataSourceSample(
                        pipelineDataSourceUsage, 0, 20, pipelineWaitingThreads),
                new SystemPerformanceReportVO.DataSourceSample(10, 0, 16, 0));
    }

    private static SystemPerformanceReportVO.PerformanceSample sampleWithRedisState(
            double inputRps,
            double outputRps,
            int seconds,
            double cpuPercent,
            long streamLag,
            long pendingCount) {
        var order = new SystemPerformanceReportVO.ChannelSample(
                inputRps,
                outputRps,
                10,
                20,
                5,
                8,
                0.15,
                0,
                10,
                0,
                0,
                0,
                streamLag,
                pendingCount);
        var payment = new SystemPerformanceReportVO.ChannelSample(
                0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0);
        return new SystemPerformanceReportVO.PerformanceSample(
                OffsetDateTime.now().plusSeconds(seconds),
                order,
                payment,
                cpuPercent,
                30,
                new SystemPerformanceReportVO.DataSourceSample(10, 0, 20, 0),
                new SystemPerformanceReportVO.DataSourceSample(10, 0, 16, 0));
    }
}
