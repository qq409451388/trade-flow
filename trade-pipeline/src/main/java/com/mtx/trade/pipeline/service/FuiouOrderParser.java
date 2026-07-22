package com.mtx.trade.pipeline.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.pipeline.config.FuiouOrderProperties;
import com.mtx.trade.pipeline.dto.OrderAggregate;
import com.mtx.trade.pipeline.dto.OrderEventMessage;
import com.mtx.trade.pipeline.entity.OrderDO;
import com.mtx.trade.pipeline.entity.OrderItemDO;
import com.mtx.trade.pipeline.entity.OrderItemSpecDO;
import com.mtx.trade.pipeline.entity.OrderPackageItemDO;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/** 将富友订单 JSON 转换为 Pipeline 最终表模型。 */
@Service
@RequiredArgsConstructor
public class FuiouOrderParser {

    private final ObjectMapper objectMapper;
    private final FuiouOrderProperties properties;

    public OrderAggregate parse(byte[] content, OrderEventMessage event) {
        if (content == null || content.length == 0) {
            throw invalid("Storage 订单原文为空");
        }
        try {
            JsonNode root = objectMapper.readTree(new String(content, StandardCharsets.UTF_8));
            if (root == null || !root.isObject()) {
                throw invalid("订单原文必须是 JSON Object");
            }

            OrderDO order = convert(root, OrderDO.class);
            order.setId(null);
            order.setStorageId(event.storageId());
            order.setPayloadSha256(event.storageSha256());
            order.setCreateTime(null);
            order.setUpdateTime(null);
            order.setOrderCreateTime(requiredTime(root, "crtTm"));
            order.setPayDeadlineTime(time(root, "payDeadlineTm"));
            order.setPayTime(time(root, "payTm"));
            order.setCashierConfirmTime(time(root, "cashierConfirmTm"));
            order.setDeliveryStartTime(time(root, "deliverStartTm"));
            order.setCommentTime(time(root, "commentTm"));
            order.setFinishTime(time(root, "finshTm"));
            order.setSourceUpdateTime(requiredTime(root, "recUpdTm"));
            order.setRefundTime(time(root, "refundTm"));
            order.setOpenTableTime(time(root, "openTableTm"));
            order.setMealTime(time(root, "mealTm"));
            order.setEstimatedFinishTime(time(root, "maybeFinshTm"));
            order.setReverseTime(time(root, "reverseTm"));
            order.setDeliverTimeDesc(text(root, "deliverTm"));
            order.setPrintSettleStatus(integer(root, "isPrintSettleList"));
            order.setThirdBasketStatus(integer(root, "thirdIsBaskets"));

            requirePositive(order.getOrderNo(), "orderNo");
            requireText(order.getMchntCd(), "mchntCd");
            long payloadVersion = epochMillis(root, "recUpdTm", true);
            if (payloadVersion != event.messageVersion()) {
                throw invalid("event.messageVersion 与原文 recUpdTm 不一致");
            }

            List<OrderItemDO> items = new ArrayList<>();
            List<OrderItemSpecDO> specs = new ArrayList<>();
            List<OrderPackageItemDO> packageItems = new ArrayList<>();
            JsonNode detailInfos = root.get("orderDetailInfos");
            if (detailInfos == null || !detailInfos.isArray()) {
                throw invalid("完整订单快照的 orderDetailInfos 必须存在且为数组");
            }
            for (JsonNode itemNode : detailInfos) {
                parseItem(itemNode, order, items, specs, packageItems);
            }
            return new OrderAggregate(order, items, specs, packageItems, event.messageVersion());
        } catch (BusinessException e) {
            throw e;
        } catch (IOException | IllegalArgumentException e) {
            throw invalid("富友订单原文解析失败: " + e.getMessage());
        }
    }

    private void parseItem(
            JsonNode itemNode,
            OrderDO order,
            List<OrderItemDO> items,
            List<OrderItemSpecDO> specs,
            List<OrderPackageItemDO> packageItems) throws IOException {
        if (itemNode == null || !itemNode.isObject()) {
            throw invalid("orderDetailInfos 元素必须是对象");
        }
        OrderItemDO item = convert(itemNode, OrderItemDO.class);
        item.setId(null);
        item.setCreateTime(null);
        item.setUpdateTime(null);
        item.setOrderNo(defaultValue(item.getOrderNo(), order.getOrderNo()));
        item.setShopId(defaultValue(item.getShopId(), order.getShopId()));
        item.setMchntCd(defaultText(item.getMchntCd(), order.getMchntCd()));
        item.setItemCreateTime(defaultValue(time(itemNode, "crtTm"), order.getOrderCreateTime()));
        item.setDishUpdateTime(defaultValue(time(itemNode, "dishUpdTm"), order.getSourceUpdateTime()));
        item.setInPkgGoodsPrice(longValue(itemNode, "inPkgGooodsPrice"));
        item.setPrepackagedAmt(longValue(itemNode, "prePackageedAmt"));
        requirePositive(item.getDetailNo(), "orderDetailInfos.detailNo");
        requirePositive(item.getOrderNo(), "orderDetailInfos.orderNo");
        requirePositive(item.getShopId(), "orderDetailInfos.shopId");
        requireText(item.getMchntCd(), "orderDetailInfos.mchntCd");
        if (item.getItemCreateTime() == null || item.getDishUpdateTime() == null) {
            throw invalid("订单商品明细缺少创建时间或更新时间");
        }
        if (item.getItemCreateTime().getYear() != order.getOrderCreateTime().getYear()) {
            throw invalid("订单商品明细与主订单跨年，无法满足父子表年度路由约束");
        }
        items.add(item);

        JsonNode specNodes = itemNode.path("orderGoodsSpecs");
        if (specNodes.isArray()) {
            for (JsonNode specNode : specNodes) {
                OrderItemSpecDO spec = convert(specNode, OrderItemSpecDO.class);
                spec.setId(null);
                spec.setCreateTime(null);
                spec.setUpdateTime(null);
                spec.setDetailNo(defaultValue(spec.getDetailNo(), item.getDetailNo()));
                requirePositive(spec.getOrderSpecId(), "orderGoodsSpecs.orderSpecId");
                requirePositive(spec.getDetailNo(), "orderGoodsSpecs.detailNo");
                specs.add(spec);
            }
        } else if (!specNodes.isMissingNode() && !specNodes.isNull()) {
            throw invalid("orderGoodsSpecs 必须是数组");
        }

        JsonNode packageNodes = itemNode.path("packageDetailList");
        if (packageNodes.isArray()) {
            for (JsonNode packageNode : packageNodes) {
                OrderPackageItemDO packageItem = convert(packageNode, OrderPackageItemDO.class);
                packageItem.setId(null);
                packageItem.setCreateTime(null);
                packageItem.setUpdateTime(null);
                packageItem.setRelatePkgDetailNo(defaultValue(
                        packageItem.getRelatePkgDetailNo(), item.getDetailNo()));
                packageItem.setOrderNo(defaultValue(packageItem.getOrderNo(), order.getOrderNo()));
                packageItem.setShopId(defaultValue(packageItem.getShopId(), order.getShopId()));
                packageItem.setMchntCd(defaultText(packageItem.getMchntCd(), order.getMchntCd()));
                packageItem.setItemCreateTime(defaultValue(
                        time(packageNode, "crtTm"), item.getItemCreateTime()));
                requirePositive(packageItem.getPackageDetailNo(), "packageDetailList.packageDetailNo");
                requirePositive(packageItem.getRelatePkgDetailNo(), "packageDetailList.relatePkgDetailNo");
                requirePositive(packageItem.getOrderNo(), "packageDetailList.orderNo");
                requirePositive(packageItem.getShopId(), "packageDetailList.shopId");
                requireText(packageItem.getMchntCd(), "packageDetailList.mchntCd");
                if (packageItem.getItemCreateTime() == null) {
                    throw invalid("套餐子商品缺少创建时间");
                }
                if (packageItem.getItemCreateTime().getYear() != order.getOrderCreateTime().getYear()) {
                    throw invalid("套餐子商品与主订单跨年，无法满足父子表年度路由约束");
                }
                packageItems.add(packageItem);
            }
        } else if (!packageNodes.isMissingNode() && !packageNodes.isNull()) {
            throw invalid("packageDetailList 必须是数组");
        }
    }

    private <T> T convert(JsonNode node, Class<T> targetType) throws IOException {
        return objectMapper.readerFor(targetType)
                .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .readValue(node);
    }

    private LocalDateTime requiredTime(JsonNode root, String name) {
        long epochMilli = epochMillis(root, name, true);
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), properties.getZoneId());
    }

    private LocalDateTime time(JsonNode root, String name) {
        long epochMilli = epochMillis(root, name, false);
        return epochMilli <= 0
                ? null
                : LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), properties.getZoneId());
    }

    private long epochMillis(JsonNode root, String name, boolean required) {
        JsonNode node = root.get(name);
        if (node == null || node.isNull() || node.asText().isBlank()) {
            if (required) {
                throw invalid(name + " 不能为空");
            }
            return 0;
        }
        long value;
        try {
            value = Long.parseLong(node.asText());
        } catch (NumberFormatException e) {
            throw invalid(name + " 必须是毫秒时间戳");
        }
        if (required && value <= 0) {
            throw invalid(name + " 必须为正数");
        }
        return value;
    }

    private static Integer integer(JsonNode root, String name) {
        JsonNode node = root.get(name);
        if (node == null || node.isNull() || node.asText().isBlank()) {
            return null;
        }
        return Integer.valueOf(node.asText());
    }

    private static Long longValue(JsonNode root, String name) {
        JsonNode node = root.get(name);
        if (node == null || node.isNull() || node.asText().isBlank()) {
            return null;
        }
        return Long.valueOf(node.asText());
    }

    private static String text(JsonNode root, String name) {
        JsonNode node = root.get(name);
        return node == null || node.isNull() ? null : node.asText();
    }

    private static <T> T defaultValue(T value, T fallback) {
        return value == null ? fallback : value;
    }

    private static String defaultText(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void requirePositive(Long value, String name) {
        if (value == null || value <= 0) {
            throw invalid(name + " 必须为正数");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw invalid(name + " 不能为空");
        }
    }

    private static BusinessException invalid(String message) {
        return new BusinessException(ErrorCode.PARAM_INVALID, message);
    }
}
