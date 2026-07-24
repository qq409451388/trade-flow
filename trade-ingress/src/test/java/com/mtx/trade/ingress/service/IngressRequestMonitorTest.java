package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.config.IngressObservabilityProperties;
import com.mtx.trade.ingress.dto.IngestRequestTrace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

@ExtendWith(OutputCaptureExtension.class)
class IngressRequestMonitorTest {

    @Test
    void shouldLogTraceAndStageDurationsWhenRequestIsSlow(CapturedOutput output) {
        IngressObservabilityProperties properties = new IngressObservabilityProperties();
        properties.setSlowRequestThreshold(Duration.ofMillis(1));
        IngressRequestMonitor monitor = new IngressRequestMonitor(properties);
        IngestRequestTrace trace = IngestRequestTrace.fromPayload("{\"order\":\"100\"}");

        monitor.logIfSlow(
                "order",
                trace,
                "PUBLISHED",
                Duration.ofMillis(10).toNanos(),
                Duration.ofMillis(1).toNanos(),
                Duration.ofMillis(2).toNanos(),
                Duration.ofMillis(1).toNanos(),
                Duration.ofMillis(5).toNanos(),
                Duration.ofMillis(1).toNanos());

        assertThat(output)
                .contains("[Ingress Slow Request]")
                .contains("channel=order")
                .contains("requestId=" + trace.requestId())
                .contains("payloadSha256=" + trace.payloadSha256Hex())
                .contains("finalStage=PUBLISHED")
                .contains("eventPersistMs=5.0");
    }

    @Test
    void shouldRejectNonPositiveThreshold() {
        IngressObservabilityProperties properties = new IngressObservabilityProperties();
        properties.setSlowRequestThreshold(Duration.ZERO);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> new IngressRequestMonitor(properties))
                .withMessageContaining("slow-request-threshold");
    }
}
