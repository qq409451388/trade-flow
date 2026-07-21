package com.mtx.trade.pipeline;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 启动上下文加载测试。
 */
@SpringBootTest(properties = "trade.storage.local.enabled=false")
class TradePipelineApplicationTests {

    @Test
    void contextLoads() {
    }
}
