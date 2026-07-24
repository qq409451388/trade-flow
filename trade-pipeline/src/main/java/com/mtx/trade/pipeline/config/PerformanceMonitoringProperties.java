package com.mtx.trade.pipeline.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** 性能报告采样窗口、容量目标和诊断阈值。 */
@Data
@ConfigurationProperties(prefix = "trade.pipeline.performance-monitoring")
public class PerformanceMonitoringProperties {

    private boolean enabled = true;
    private long sampleIntervalMs = 5_000;
    private int historySize = 60;
    private double orderTargetRps = 100D;
    private double paymentTargetRps = 100D;
    private double queueWarningPercent = 50D;
    private double cpuWarningPercent = 80D;
    private double dataSourceWarningPercent = 90D;
}
