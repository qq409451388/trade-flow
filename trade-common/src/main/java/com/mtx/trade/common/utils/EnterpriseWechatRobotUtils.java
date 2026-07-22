package com.mtx.trade.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * 企业微信机器人消息工具。
 *
 * @author codex
 */
@Slf4j
@Component
public class EnterpriseWechatRobotUtils {
    private static final String DEFAULT_WEBHOOK_URL =
            "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=b749242a-c93d-44c6-9bd5-8792c0d1ccdb";

    private final SpringUtils springUtils;
    private final HttpClient httpClient;

    public EnterpriseWechatRobotUtils(SpringUtils springUtils) {
        this.springUtils = springUtils;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    /**
     * 使用默认机器人发送文本消息。
     *
     * @author codex
     */
    public void sendText(String content) {
        sendText(DEFAULT_WEBHOOK_URL, content);
    }

    /**
     * 发送企业微信机器人文本消息。
     *
     * @author codex
     */
    public void sendText(String webhookUrl, String content) {
        if (StringUtils.isBlank(webhookUrl) || StringUtils.isBlank(content)) {
            return;
        }
        String body = "{\"msgtype\":\"text\",\"text\":{\"content\":\""
                + escapeJson(content) + "\"}}";
        HttpRequest request = HttpRequest.newBuilder(URI.create(webhookUrl))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        try {
            HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("Enterprise WeChat HTTP status=" + response.statusCode());
            }
            String normalizedBody = response.body() == null
                    ? "" : response.body().replaceAll("\\s+", "");
            if (!normalizedBody.contains("\"errcode\":0")) {
                throw new IllegalStateException("Enterprise WeChat rejected the message: " + response.body());
            }
            log.info("[Enterprise WeChat] ✅ Robot notification was delivered. response={}", response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Enterprise WeChat request was interrupted", e);
        } catch (Exception e) {
            throw new IllegalStateException("Enterprise WeChat request failed", e);
        }
    }

    /**
     * 安静发送企业微信机器人文本消息，失败不影响主流程。
     *
     * @author codex
     */
    public void sendTextQuietly(String content) {
        if (!springUtils.isProd()) {
            log.warn("[Enterprise WeChat] 🔄 Non-production notification was logged instead of sent. content={}",
                    content);
            return;
        }
        try {
            sendText(content);
        } catch (Exception exception) {
            log.warn("[Enterprise WeChat] 🔄 Robot notification failed; the terminal failure remains in audit. "
                    + "reason={}", exception.getMessage());
        }
    }

    private static String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 16);
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            switch (current) {
                case '\\' -> escaped.append("\\\\");
                case '"' -> escaped.append("\\\"");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (current < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) current));
                    } else {
                        escaped.append(current);
                    }
                }
            }
        }
        return escaped.toString();
    }
}
