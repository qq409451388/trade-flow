package com.mtx.trade.analytics;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 交易分析服务启动类。
 */
@SpringBootApplication
@MapperScan("com.mtx.trade.analytics.mapper")
public class TradeAnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(TradeAnalyticsApplication.class, args);
    }
}
