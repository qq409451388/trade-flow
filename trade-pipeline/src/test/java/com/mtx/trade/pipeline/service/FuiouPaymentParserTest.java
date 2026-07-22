package com.mtx.trade.pipeline.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.config.FuiouPaymentProperties;
import com.mtx.trade.pipeline.dto.PaymentEventMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FuiouPaymentParserTest {

    private static final String PAY_SSN = "SAAS35123509202607091758495861";
    private static final String PAY_TIME = "2025-01-01 00:00:00";

    @Test
    void shouldValidateMessageVersionAgainstPaymentTime() {
        long version = paymentTimeVersion();
        FuiouPaymentParser parser = new FuiouPaymentParser(new ObjectMapper(), new FuiouPaymentProperties());

        var aggregate = parser.parse(payload(), event(version), LocalDateTime.now());

        assertThat(aggregate.payment().getPaySsn()).isEqualTo(PAY_SSN);
        assertThat(aggregate.payment().getPayTime()).isEqualTo(LocalDateTime.of(2025, 1, 1, 0, 0));
    }

    @Test
    void shouldRejectMessageVersionDifferentFromPaymentTime() {
        FuiouPaymentParser parser = new FuiouPaymentParser(new ObjectMapper(), new FuiouPaymentProperties());

        assertThatThrownBy(() -> parser.parse(payload(), event(paymentTimeVersion() + 1), LocalDateTime.now()))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("payTm");
    }

    private static byte[] payload() {
        return ("{\"paySsn\":\"" + PAY_SSN + "\",\"payTm\":\"" + PAY_TIME
                + "\",\"mchntCd\":\"M100\",\"payState\":1}")
                .getBytes(StandardCharsets.UTF_8);
    }

    private static PaymentEventMessage event(long version) {
        return new PaymentEventMessage(
                1L, 2L, new byte[32], PAY_SSN, 1, ContentType.PAYMENT.getCode(), version);
    }

    private static long paymentTimeVersion() {
        return LocalDateTime.of(2025, 1, 1, 0, 0)
                .atZone(ZoneId.of("Asia/Shanghai"))
                .toInstant()
                .toEpochMilli();
    }
}
