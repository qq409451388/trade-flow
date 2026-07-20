package com.mtx.trade.analytics.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 轻量级健康检查端点，便于网关/负载均衡探活。
 * 完整健康信息见 Actuator: /actuator/health
 */
@RestController
public class HealthController {

    @GetMapping("/ping")
    public Map<String, Object> ping() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "UP");
        result.put("service", "trade-analytics");
        result.put("timestamp", LocalDateTime.now().toString());
        return result;
    }
}
