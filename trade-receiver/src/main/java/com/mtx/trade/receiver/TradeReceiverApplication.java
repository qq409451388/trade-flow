package com.mtx.trade.receiver;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan({"com.mtx.trade.receiver.mapper","com.mtx.trade.common.storage.mapper"})
public class TradeReceiverApplication {
    public static void main(String[] args) {
        SpringApplication.run(TradeReceiverApplication.class, args);
    }
}
