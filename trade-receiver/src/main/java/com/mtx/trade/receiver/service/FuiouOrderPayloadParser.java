package com.mtx.trade.receiver.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.receiver.config.FuiouOrderPayloadProperties;
import com.mtx.trade.receiver.dto.ParsedEventVersion;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/** 从富友订单 JSON 原文中提取事件键和消息版本。 */
@Component
@RequiredArgsConstructor
public class FuiouOrderPayloadParser {

    private final ObjectMapper objectMapper;
    private final FuiouOrderPayloadProperties properties;

    public ParsedEventVersion parse(String payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);
            if (root == null) {
                throw new BusinessException(ErrorCode.PARAM_INVALID, "富友报文不能为空");
            }
            String eventKey = textAt(root, properties.getEventKeyPointer(), "第三方事件键");
            String versionText = textAt(root, properties.getMessageVersionPointer(), "消息版本");
            long messageVersion = Long.parseLong(versionText);
            if (messageVersion < 0) {
                throw new NumberFormatException("negative version");
            }
            return new ParsedEventVersion(eventKey, messageVersion);
        } catch (JsonProcessingException | NumberFormatException e) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "富友报文事件键或消息版本格式错误");
        }
    }

    private static String textAt(JsonNode root, String pointer, String fieldName) {
        JsonNode value = root.at(pointer);
        String text = value.isValueNode() ? value.asText() : null;
        if (!StringUtils.hasText(text)) {
            throw new BusinessException(ErrorCode.PARAM_INVALID, "富友报文缺少" + fieldName + "：" + pointer);
        }
        return text;
    }
}
