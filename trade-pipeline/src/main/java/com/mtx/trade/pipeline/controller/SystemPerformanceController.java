package com.mtx.trade.pipeline.controller;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.pipeline.dto.SystemPerformanceReportVO;
import com.mtx.trade.pipeline.service.SystemPerformanceService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 压测和容量评估使用的 Pipeline 当前性能快照。 */
@RestController
@RequestMapping("/performance")
@RequiredArgsConstructor
public class SystemPerformanceController {

    private final SystemPerformanceService performanceService;

    @GetMapping(value = "/current", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseData<SystemPerformanceReportVO> current() {
        return ResponseData.success(performanceService.current());
    }
}
