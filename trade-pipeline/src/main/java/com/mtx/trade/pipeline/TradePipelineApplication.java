package com.mtx.trade.pipeline;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 交易流水处理服务启动类。
 */
@SpringBootApplication
@MapperScan("com.mtx.trade.pipeline.mapper")
public class TradePipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(TradePipelineApplication.class, args);
    }
}
