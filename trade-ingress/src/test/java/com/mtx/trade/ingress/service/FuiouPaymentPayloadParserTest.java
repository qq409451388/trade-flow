package com.mtx.trade.ingress.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.config.FuiouPaymentPayloadProperties;
import com.mtx.trade.ingress.dto.ParsedEventVersion;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FuiouPaymentPayloadParserTest {

    @Test
    void shouldUsePaymentTimeEpochMillisAsMessageVersion() {
        FuiouPaymentPayloadParser parser = new FuiouPaymentPayloadParser(
                new ObjectMapper(), new FuiouPaymentPayloadProperties());

        ParsedEventVersion result = parser.parse("""
                {"paySsn":"SAAS35123509202607091758495861","payTm":"2025-01-01 00:00:00"}
                """);

        long expected = LocalDateTime.of(2025, 1, 1, 0, 0)
                .atZone(ZoneId.of("Asia/Shanghai"))
                .toInstant()
                .toEpochMilli();
        assertThat(result.eventKey()).isEqualTo("SAAS35123509202607091758495861");
        assertThat(result.messageVersion()).isEqualTo(expected);
    }

    @Test
    void shouldRejectInvalidPaymentTime() {
        FuiouPaymentPayloadParser parser = new FuiouPaymentPayloadParser(
                new ObjectMapper(), new FuiouPaymentPayloadProperties());

        assertThatThrownBy(() -> parser.parse("""
                {"paySsn":"SAAS35123509202607091758495861","payTm":"invalid"}
                """))
                .isInstanceOf(BusinessException.class);
    }
}
