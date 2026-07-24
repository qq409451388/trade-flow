package com.mtx.trade.ingress.dto;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IngestRequestTraceTest {

    @Test
    void shouldCreateRequestIdAndFullPayloadSha256() {
        IngestRequestTrace trace = IngestRequestTrace.fromPayload("test");

        assertThat(trace.requestId()).hasSize(32).containsPattern("[0-9a-f]{32}");
        assertThat(trace.payloadSha256()).hasSize(32);
        assertThat(trace.payloadSha256Hex())
                .isEqualTo("9f86d081884c7d659a2feaa0c55ad015"
                        + "a3bf4f1b2b0b822cd15d6c15b0f00a08");
    }
}
