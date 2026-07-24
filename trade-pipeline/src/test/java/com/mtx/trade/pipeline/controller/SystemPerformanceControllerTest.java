package com.mtx.trade.pipeline.controller;

import com.mtx.trade.pipeline.dto.SystemPerformanceReportVO;
import com.mtx.trade.pipeline.service.SystemPerformanceService;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SystemPerformanceControllerTest {

    @Test
    void shouldReturnStandardJsonResponse() throws Exception {
        SystemPerformanceReportVO report = new SystemPerformanceReportVO(
                "test", null, null, null, null, List.of(), null, Map.of());
        SystemPerformanceService service = new SystemPerformanceService(
                null, null, null, null, null, null, null, null, null, null) {
            @Override
            public SystemPerformanceReportVO current() {
                return report;
            }
        };

        var response = new SystemPerformanceController(service).current();
        Method method = SystemPerformanceController.class.getMethod("current");
        GetMapping mapping = method.getAnnotation(GetMapping.class);

        assertThat(response.getCode()).isZero();
        assertThat(response.getData()).isSameAs(report);
        assertThat(mapping.produces()).containsExactly(MediaType.APPLICATION_JSON_VALUE);
    }
}
