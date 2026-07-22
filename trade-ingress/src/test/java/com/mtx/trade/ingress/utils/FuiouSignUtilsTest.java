package com.mtx.trade.ingress.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FuiouSignUtilsTest {

    @Test
    void shouldFailClosedWhenSecretIsMissing() {
        FuiouSignUtils.FuiouSignParts parts =
                new FuiouSignUtils.FuiouSignParts("0123456789abcdef0123456789abcdef", "{}");

        assertThat(FuiouSignUtils.verifySign(parts, null)).isFalse();
        assertThat(FuiouSignUtils.verifySign(parts, " ")).isFalse();
    }

    @Test
    void shouldFailWhenSignatureIsMissing() {
        FuiouSignUtils.FuiouSignParts parts = new FuiouSignUtils.FuiouSignParts("", "{}");

        assertThat(FuiouSignUtils.verifySign(parts, "secret")).isFalse();
    }
}
