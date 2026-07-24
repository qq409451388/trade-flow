package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.config.IngressObservabilityProperties;
import com.mtx.trade.ingress.dto.IngestRequestTrace;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;

/** 对最终成功但响应缓慢的接入请求记录分阶段耗时。 */
@Slf4j
@Component
public class IngressRequestMonitor {

    private final IngressObservabilityProperties properties;

    public IngressRequestMonitor(IngressObservabilityProperties properties) {
        Duration threshold = properties.getSlowRequestThreshold();
        if (threshold == null || threshold.isZero() || threshold.isNegative()) {
            throw new IllegalArgumentException(
                    "trade.ingress.observability.slow-request-threshold必须为正数");
        }
        this.properties = properties;
    }

    public void logIfSlow(
            String channel,
            IngestRequestTrace trace,
            String finalStage,
            long totalNanos,
            long signatureNanos,
            long storageNanos,
            long eventParseNanos,
            long eventPersistNanos,
            long publishNanos) {
        Duration threshold = properties.getSlowRequestThreshold();
        if (totalNanos < threshold.toNanos()) {
            return;
        }
        log.warn("[Ingress Slow Request] Request completed slowly. channel={}, requestId={}, "
                        + "payloadSha256={}, finalStage={}, totalMs={}, signatureMs={}, storageMs={}, "
                        + "eventParseMs={}, eventPersistMs={}, publishMs={}",
                channel,
                trace.requestId(),
                trace.payloadSha256Hex(),
                finalStage,
                millis(totalNanos),
                millis(signatureNanos),
                millis(storageNanos),
                millis(eventParseNanos),
                millis(eventPersistNanos),
                millis(publishNanos));
    }

    private static double millis(long nanos) {
        return nanos / 1_000_000D;
    }
}
