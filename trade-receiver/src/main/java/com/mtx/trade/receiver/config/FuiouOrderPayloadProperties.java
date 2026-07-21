package com.mtx.trade.receiver.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** 富友订单报文业务字段位置，使用 Jackson JSON Pointer 表达。 */
@Component
@ConfigurationProperties("trade.thirdparty.fuiou.order-payload")
public class FuiouOrderPayloadProperties {

    private String eventKeyPointer = "/orderNo";
    private String messageVersionPointer = "/version";

    public String getEventKeyPointer() {
        return eventKeyPointer;
    }

    public void setEventKeyPointer(String eventKeyPointer) {
        this.eventKeyPointer = eventKeyPointer;
    }

    public String getMessageVersionPointer() {
        return messageVersionPointer;
    }

    public void setMessageVersionPointer(String messageVersionPointer) {
        this.messageVersionPointer = messageVersionPointer;
    }
}
