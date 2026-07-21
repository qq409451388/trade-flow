package com.mtx.trade.pipeline;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 交易流水处理服务启动类。
 */
@SpringBootApplication
@MapperScan(basePackages = "com.mtx.trade.pipeline.mapper", sqlSessionFactoryRef = "sqlSessionFactory")
public class TradePipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(TradePipelineApplication.class, args);
    }
}
