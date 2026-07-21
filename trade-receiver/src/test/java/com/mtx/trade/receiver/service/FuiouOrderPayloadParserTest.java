package com.mtx.trade.receiver.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.receiver.config.FuiouOrderPayloadProperties;
import com.mtx.trade.receiver.dto.ParsedEventVersion;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FuiouOrderPayloadParserTest {

    @Test
    void shouldExtractConfiguredNestedFields() {
        FuiouOrderPayloadProperties properties = new FuiouOrderPayloadProperties();
        properties.setEventKeyPointer("/data/orderNo");
        properties.setMessageVersionPointer("/data/version");
        FuiouOrderPayloadParser parser = new FuiouOrderPayloadParser(new ObjectMapper(), properties);

        ParsedEventVersion result = parser.parse("{\"data\":{\"orderNo\":\"O-100\",\"version\":12}}");

        assertThat(result.eventKey()).isEqualTo("O-100");
        assertThat(result.messageVersion()).isEqualTo(12L);
    }

    @Test
    void shouldRejectMissingOrNegativeVersion() {
        FuiouOrderPayloadParser parser = new FuiouOrderPayloadParser(
                new ObjectMapper(), new FuiouOrderPayloadProperties());

        assertThatThrownBy(() -> parser.parse("{\"orderNo\":\"O-100\"}"))
                .isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> parser.parse("{\"orderNo\":\"O-100\",\"version\":-1}"))
                .isInstanceOf(BusinessException.class);
    }
}
