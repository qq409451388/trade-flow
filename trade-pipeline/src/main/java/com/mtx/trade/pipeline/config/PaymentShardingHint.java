package com.mtx.trade.pipeline.config;

import org.apache.shardingsphere.infra.hint.HintManager;

/** 支付主表和账户表共享的强制年份路由上下文。 */
public final class PaymentShardingHint {

    private PaymentShardingHint() {
    }

    public static HintManager useYear(int routeYear) {
        if (routeYear < 2000 || routeYear > 9999) {
            throw new IllegalArgumentException("invalid payment route year: " + routeYear);
        }
        HintManager hintManager = HintManager.getInstance();
        hintManager.addTableShardingValue(PipelineShardingRuleFactory.PAYMENT_TABLE, routeYear);
        hintManager.addTableShardingValue(PipelineShardingRuleFactory.PAYMENT_ACCOUNT_TABLE, routeYear);
        return hintManager;
    }
}
