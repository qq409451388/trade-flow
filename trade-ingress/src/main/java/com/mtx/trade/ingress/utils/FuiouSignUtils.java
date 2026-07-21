package com.mtx.trade.ingress.utils;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FuiouSignUtils {
    /** keySign 签名正则：富友报文里 keySign 为 32 位 MD5 十六进制，末尾逗号可选（keySign 可能是最后一个字段） */
    private static final Pattern KEY_SIGN_PATTERN =
            Pattern.compile("\"keySign\"\\s*:\\s*\"([0-9a-fA-F]{32})\"");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static boolean verifySign(FuiouSignParts parts, String secret) {
        String keySign = parts.keySign();
        String bodyWithoutSign = parts.bodyWithoutSign();
        if (!StringUtils.hasText(keySign) || !StringUtils.hasText(secret)) {
            // secret 未配置时放行，便于联调测试
            return !StringUtils.hasText(secret);
        }
        String content = secret + bodyWithoutSign;
        String hash = DigestUtils.md5DigestAsHex(content.getBytes(StandardCharsets.UTF_8));
        return hash.equalsIgnoreCase(keySign);
    }

    public static FuiouSignParts parseSign(String body) {
        try {
            Matcher matcher = KEY_SIGN_PATTERN.matcher(body);
            if (!matcher.find()) {
                return parseSignFallback(body);
            }
            // 获取签名
            String keySign = matcher.group(1);

            String bodyWithoutSign = removeKeySignField(body, matcher.start(), matcher.end());
            return new FuiouSignParts(keySign, bodyWithoutSign);
        } catch (Exception ex) {
            throw new BusinessException(ErrorCode.BUSINESS_ERROR);
        }
    }

    private static FuiouSignParts parseSignFallback(String body) throws JsonProcessingException {
        try (JsonParser parser = OBJECT_MAPPER.getFactory().createParser(body)) {
            while (parser.nextToken() != null) {
                if (parser.currentToken() != JsonToken.FIELD_NAME
                        || !"keySign".equals(parser.currentName())) {
                    continue;
                }

                JsonLocation fieldStart = parser.currentTokenLocation();
                JsonToken valueToken = parser.nextToken();
                if (valueToken == null || !valueToken.isScalarValue()) {
                    throw new IllegalArgumentException("keySign 必须是标量值");
                }

                String keySign = parser.getValueAsString();
                if (!StringUtils.hasText(keySign)) {
                    throw new IllegalArgumentException("keySign 为空");
                }

                // currentLocation 指向当前值之后；用原始字符偏移删除，避免 JSON 重序列化改变验签原文。
                int start = toCharOffset(fieldStart, body);
                int end = toCharOffset(parser.currentLocation(), body);
                return new FuiouSignParts(keySign, removeKeySignField(body, start, end));
            }
            throw new IllegalArgumentException("缺少keySign");
        } catch (JsonProcessingException e) {
            throw e;
        } catch (IOException e) {
            throw new IllegalArgumentException("解析签名报文失败", e);
        }
    }

    private static int toCharOffset(JsonLocation location, String body) {
        long offset = location.getCharOffset();
        if (offset < 0 || offset > body.length()) {
            throw new IllegalArgumentException("无法定位 keySign 字段");
        }
        return (int) offset;
    }

    /**
     * 从原始 JSON 中移除一个完整字段，同时删除相邻的一个分隔逗号。
     * 其余空白、字段顺序和字符编码均保持不变，确保参与验签的原文不被重写。
     */
    private static String removeKeySignField(String body, int start, int end) {
        int removeStart = start;
        int removeEnd = end;

        int before = start - 1;
        while (before >= 0 && Character.isWhitespace(body.charAt(before))) {
            before--;
        }
        if (before >= 0 && body.charAt(before) == ',') {
            removeStart = before;
        } else {
            int after = end;
            while (after < body.length() && Character.isWhitespace(body.charAt(after))) {
                after++;
            }
            if (after < body.length() && body.charAt(after) == ',') {
                removeEnd = after + 1;
            }
        }
        return body.substring(0, removeStart) + body.substring(removeEnd);
    }

    public record FuiouSignParts (String keySign, String bodyWithoutSign) {};
}
