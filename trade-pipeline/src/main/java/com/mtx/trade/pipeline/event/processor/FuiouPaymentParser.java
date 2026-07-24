package com.mtx.trade.pipeline.event.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.config.FuiouPaymentProperties;
import com.mtx.trade.pipeline.dto.PaymentAggregate;
import com.mtx.trade.pipeline.dto.PaymentEventMessage;
import com.mtx.trade.pipeline.entity.PaymentAccountDO;
import com.mtx.trade.pipeline.entity.PaymentDO;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.List;

/** 严格解析富友支付推送，未知字段仅保留在 Storage。 */
@Service
@RequiredArgsConstructor
public class FuiouPaymentParser {

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter
            .ofPattern("uuuu-MM-dd HH:mm:ss")
            .withResolverStyle(ResolverStyle.STRICT);

    private final ObjectMapper objectMapper;
    private final FuiouPaymentProperties properties;

    public PaymentAggregate parse(
            byte[] content,
            PaymentEventMessage event,
            LocalDateTime receivedTime) {
        if (content == null || content.length == 0) {
            throw invalid("Storage 支付原文为空");
        }
        try {
            JsonNode root = objectMapper.readTree(new String(content, StandardCharsets.UTF_8));
            if (root == null || !root.isObject()) {
                throw invalid("支付原文必须是 JSON Object");
            }

            PaymentDO payment = new PaymentDO();
            payment.setPaySsn(requiredText(root, "paySsn"));
            if (!payment.getPaySsn().equals(event.eventKey())) {
                throw invalid("event.eventKey 与原文 paySsn 不一致");
            }
            payment.setSourcePaySsn(text(root, "sourcePaySsn"));
            payment.setOrderNo(longValue(root, "orderNo", 0L));
            payment.setMchntCd(requiredText(root, "mchntCd"));
            payment.setShopId(longValue(root, "shopId", 0L));
            payment.setShopName(text(root, "shopName"));
            payment.setPayTime(requiredTime(root, "payTm"));
            long payloadVersion = payment.getPayTime()
                    .atZone(properties.getZoneId())
                    .toInstant()
                    .toEpochMilli();
            if (payloadVersion != event.messageVersion()) {
                throw invalid("event.messageVersion 与原文 payTm 不一致");
            }
            payment.setRefundTime(time(root, "refundTm"));
            payment.setPayType(text(root, "payType"));
            payment.setPayName(text(root, "payName"));
            payment.setPayState(integer(root, "payState", null));
            payment.setPayAmt(longValue(root, "payAmt", 0L));
            payment.setFeeAmt(longValue(root, "feeAmt", 0L));
            payment.setRefundAmt(longValue(root, "refundAmt", 0L));
            payment.setBalanceDisAmt(longValue(root, "balanceDisAmt", 0L));
            payment.setFaceAmt(longValue(root, "faceAmt", 0L));
            payment.setFySettle(booleanInt(root, "fySettle"));
            payment.setThirdOrderNo(text(root, "thirdOrderNo"));
            payment.setChannelTradeNo(text(root, "channelTradeNo"));
            payment.setBigCategory(text(root, "bigCategory"));
            payment.setSmallCategory(text(root, "smallCategory"));
            payment.setOrderSource(text(root, "orderSource"));
            payment.setOpenId(text(root, "openId"));
            payment.setOrderType(text(root, "orderType"));
            payment.setChannelType(text(root, "channelType"));
            payment.setMemberName(text(root, "memberName"));
            payment.setPhone(text(root, "phone"));
            payment.setMemberCardNo(text(root, "memberCardNo"));
            payment.setMemberLevel(text(root, "memberLevel"));
            payment.setStorageId(event.storageId());
            payment.setPayloadSha256(event.storageSha256());
            validate(payment);

            JsonNode accountNodes = root.get("acntNoInfoList");
            if (accountNodes != null && !accountNodes.isNull() && !accountNodes.isArray()) {
                throw invalid("acntNoInfoList 必须是数组");
            }
            List<PaymentAccountDO> accounts = new ArrayList<>();
            if (accountNodes != null && accountNodes.isArray()) {
                for (int index = 0; index < accountNodes.size(); index++) {
                    JsonNode node = accountNodes.get(index);
                    if (node == null || !node.isObject()) {
                        throw invalid("acntNoInfoList[" + index + "] 必须是对象");
                    }
                    PaymentAccountDO account = new PaymentAccountDO();
                    account.setPaySsn(payment.getPaySsn());
                    account.setAccountSeq(index);
                    account.setCustAcntTp(text(node, "custAcntTp"));
                    account.setMchntCd(text(node, "mchntCd"));
                    account.setShopId(longValue(node, "shopId", 0L));
                    account.setOutAcntNm(text(node, "outAcntNm"));
                    account.setOutAcntNo(text(node, "outAcntNo"));
                    accounts.add(account);
                }
            }
            return new PaymentAggregate(payment, List.copyOf(accounts), receivedTime);
        } catch (BusinessException e) {
            throw e;
        } catch (IOException | IllegalArgumentException e) {
            throw invalid("富友支付原文解析失败: " + e.getMessage());
        }
    }

    private static void validate(PaymentDO payment) {
        if (payment.getPayState() == null || payment.getPayState() != 1 && payment.getPayState() != 2) {
            throw invalid("payState 只支持1支付成功或2退款成功");
        }
        if (payment.getPayState() == 1 && payment.getRefundTime() != null) {
            throw invalid("支付成功流水 refundTm 必须为空");
        }
        if (payment.getPayState() == 2) {
            if (payment.getSourcePaySsn() == null || payment.getSourcePaySsn().isBlank()) {
                throw invalid("退款流水 sourcePaySsn 不能为空");
            }
            if (payment.getRefundTime() == null) {
                throw invalid("退款流水 refundTm 不能为空");
            }
        }
    }

    private static LocalDateTime requiredTime(JsonNode root, String name) {
        String value = requiredText(root, name);
        try {
            return LocalDateTime.parse(value, TIME_FORMATTER);
        } catch (DateTimeParseException e) {
            throw invalid(name + " 必须符合 yyyy-MM-dd HH:mm:ss");
        }
    }

    private static LocalDateTime time(JsonNode root, String name) {
        String value = text(root, name);
        if (value.isBlank()) {
            return null;
        }
        try {
            return LocalDateTime.parse(value, TIME_FORMATTER);
        } catch (DateTimeParseException e) {
            throw invalid(name + " 必须符合 yyyy-MM-dd HH:mm:ss");
        }
    }

    private static String requiredText(JsonNode root, String name) {
        String value = text(root, name);
        if (value.isBlank()) {
            throw invalid(name + " 不能为空");
        }
        return value;
    }

    private static String text(JsonNode root, String name) {
        JsonNode node = root.get(name);
        return node == null || node.isNull() ? "" : node.asText();
    }

    private static Long longValue(JsonNode root, String name, Long defaultValue) {
        String value = text(root, name);
        if (value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException e) {
            throw invalid(name + " 必须是整数");
        }
    }

    private static Integer integer(JsonNode root, String name, Integer defaultValue) {
        String value = text(root, name);
        if (value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            throw invalid(name + " 必须是整数");
        }
    }

    private static Integer booleanInt(JsonNode root, String name) {
        JsonNode node = root.get(name);
        if (node == null || node.isNull() || node.asText().isBlank()) {
            return 0;
        }
        if (node.isBoolean()) {
            return node.booleanValue() ? 1 : 0;
        }
        String value = node.asText();
        if ("1".equals(value) || "true".equalsIgnoreCase(value)) {
            return 1;
        }
        if ("0".equals(value) || "false".equalsIgnoreCase(value)) {
            return 0;
        }
        throw invalid(name + " 必须是布尔值");
    }

    private static BusinessException invalid(String message) {
        return new BusinessException(ErrorCode.PARAM_INVALID, message);
    }
}
