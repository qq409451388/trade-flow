package com.mtx.trade.pipeline.controller;

import com.mtx.trade.common.dto.ResponseData;
import com.mtx.trade.pipeline.dto.EventConsumerReadinessVO;
import com.mtx.trade.pipeline.service.EventConsumerReadinessService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Ingress熔断恢复专用就绪检查。 */
@RestController
@RequestMapping("/readiness")
@RequiredArgsConstructor
public class EventConsumerReadinessController {

    private final EventConsumerReadinessService readinessService;

    @GetMapping("/event-consumer")
    public ResponseData<EventConsumerReadinessVO> eventConsumer(@RequestParam Integer contentType) {
        return ResponseData.success(readinessService.check(contentType));
    }
}
