package com.mtx.trade.ingress.dto;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

/** 接入请求的稳定排障标识；不保存原始报文。 */
public record IngestRequestTrace(String requestId, byte[] payloadSha256) {

    public static IngestRequestTrace fromPayload(String payload) {
        try {
            byte[] content = payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8);
            byte[] sha256 = MessageDigest.getInstance("SHA-256").digest(content);
            String requestId = UUID.randomUUID().toString().replace("-", "");
            return new IngestRequestTrace(requestId, sha256);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }

    public String payloadSha256Hex() {
        return HexFormat.of().formatHex(payloadSha256);
    }
}
