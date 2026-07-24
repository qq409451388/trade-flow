package com.mtx.trade.pipeline.task;

import com.mtx.trade.pipeline.service.SystemPerformanceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import static com.mtx.trade.pipeline.config.EventConsumerConfiguration.PERFORMANCE_MONITOR_SCHEDULER;

/** 后台保留最近性能窗口，使单次接口响应也包含连续压测上下文。 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(
        prefix = "trade.pipeline.performance-monitoring",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class SystemPerformanceSamplingTask {

    private final SystemPerformanceService performanceService;

    @Scheduled(
            initialDelayString = "${trade.pipeline.performance-monitoring.sample-interval-ms:5000}",
            fixedDelayString = "${trade.pipeline.performance-monitoring.sample-interval-ms:5000}",
            scheduler = PERFORMANCE_MONITOR_SCHEDULER)
    public void sample() {
        try {
            performanceService.sample();
        } catch (RuntimeException failure) {
            log.warn("[Performance Monitor] 🔄 Performance sample was skipped; the next scheduled sample will "
                    + "retry. error={}", failure.getClass().getSimpleName());
        }
    }
}
