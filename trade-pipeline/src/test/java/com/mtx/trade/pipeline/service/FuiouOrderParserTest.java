package com.mtx.trade.pipeline.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.pipeline.config.FuiouOrderProperties;
import com.mtx.trade.pipeline.dto.OrderEventMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

class FuiouOrderParserTest {

    @Test
    void shouldCopyStorageReferenceToOrderSnapshot() {
        byte[] sha256 = new byte[32];
        sha256[0] = 1;
        long version = LocalDateTime.of(2025, 1, 1, 0, 0)
                .atZone(ZoneId.of("Asia/Shanghai"))
                .toInstant()
                .toEpochMilli();
        OrderEventMessage event = new OrderEventMessage(
                1L, 2L, sha256, "100", 1, ContentType.ORDER.getCode(), version);
        FuiouOrderParser parser = new FuiouOrderParser(new ObjectMapper(), new FuiouOrderProperties());

        var aggregate = parser.parse(payload(version), event);

        assertThat(aggregate.order().getStorageId()).isEqualTo(2L);
        assertThat(aggregate.order().getPayloadSha256()).containsExactly(sha256);
    }

    private static byte[] payload(long version) {
        return ("{\"orderNo\":100,\"mchntCd\":\"M100\","
                + "\"crtTm\":" + version + ","
                + "\"recUpdTm\":" + version + "}")
                .getBytes(StandardCharsets.UTF_8);
    }
}
