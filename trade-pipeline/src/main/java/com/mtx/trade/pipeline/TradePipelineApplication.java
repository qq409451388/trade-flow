package com.mtx.trade.pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 交易流水处理服务启动类。
 */
@SpringBootApplication
@EnableScheduling
public class TradePipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(TradePipelineApplication.class, args);
    }
}
