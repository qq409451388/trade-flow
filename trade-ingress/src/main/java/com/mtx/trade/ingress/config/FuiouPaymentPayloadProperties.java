package com.mtx.trade.ingress.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.ZoneId;

/** 富友支付报文业务字段位置，使用 Jackson JSON Pointer 表达。 */
@Component
@ConfigurationProperties("trade.thirdparty.fuiou.payment-payload")
public class FuiouPaymentPayloadProperties {

    private String eventKeyPointer = "/paySsn";
    private String messageVersionPointer = "/payTm";
    private ZoneId zoneId = ZoneId.of("Asia/Shanghai");

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

    public ZoneId getZoneId() {
        return zoneId;
    }

    public void setZoneId(ZoneId zoneId) {
        this.zoneId = zoneId;
    }
}
