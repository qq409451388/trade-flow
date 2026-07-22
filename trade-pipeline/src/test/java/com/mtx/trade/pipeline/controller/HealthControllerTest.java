package com.mtx.trade.pipeline.controller;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class HealthControllerTest {

    @Test
    void shouldReturnStandardResponseWithOffsetTimestamp() {
        var response = new HealthController().ping();

        assertThat(response.getCode()).isZero();
        assertThat(response.getData()).containsEntry("status", "UP");
        assertThat(response.getData()).containsEntry("service", "trade-pipeline");
        assertThatCode(() -> OffsetDateTime.parse(response.getData().get("timestamp").toString()))
                .doesNotThrowAnyException();
    }
}
