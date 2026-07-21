package com.mtx.trade.ingress;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.mtx.trade.ingress.mapper", sqlSessionFactoryRef = "sqlSessionFactory")
public class TradeIngressApplication {
    public static void main(String[] args) {
        SpringApplication.run(TradeIngressApplication.class, args);
    }
}
