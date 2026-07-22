package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.storage.api.StorageMetadata;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StorageContentValidatorTest {

    @Test
    void shouldAcceptMatchingPayloadLength() {
        StorageMetadata metadata = metadata(3);

        assertThatCode(() -> StorageContentValidator.requireComplete(
                metadata, new byte[]{1, 2, 3}, "订单"))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectTruncatedContent() {
        StorageMetadata metadata = metadata(3);

        assertThatThrownBy(() -> StorageContentValidator.requireComplete(
                metadata, new byte[]{1, 2}, "支付"))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("expected=3, actual=2");
    }

    private static StorageMetadata metadata(int payloadLength) {
        return new StorageMetadata(1L, 1, 1, new byte[32], payloadLength, LocalDateTime.now());
    }
}
