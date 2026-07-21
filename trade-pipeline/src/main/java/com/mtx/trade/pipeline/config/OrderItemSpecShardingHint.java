package com.mtx.trade.pipeline.config;

import org.apache.shardingsphere.infra.hint.HintManager;

import java.time.LocalDateTime;

/** oms_order_item_spec 单表操作使用的显式年份路由上下文。 */
public final class OrderItemSpecShardingHint {

    private OrderItemSpecShardingHint() {
    }

    public static HintManager useYear(int orderYear) {
        HintManager hintManager = HintManager.getInstance();
        hintManager.addTableShardingValue(PipelineShardingRuleFactory.ORDER_ITEM_SPEC_TABLE, orderYear);
        return hintManager;
    }

    public static HintManager useOrderCreateTime(LocalDateTime orderCreateTime) {
        if (orderCreateTime == null) {
            throw new IllegalArgumentException("orderCreateTime must not be null");
        }
        return useYear(orderCreateTime.getYear());
    }
}
