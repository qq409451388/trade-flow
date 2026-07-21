package com.mtx.trade.pipeline.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/** Pipeline 订单物理分表配置。 */
@ConfigurationProperties(prefix = "trade.pipeline.sharding")
public class PipelineShardingProperties {

    /** 已实际建表并允许路由的订单年份。 */
    private List<Integer> years = new ArrayList<>(List.of(2026));

    /** 订单商品明细的 Hash 分片数。 */
    private int orderItemShardCount = 16;

    /** 是否打印 ShardingSphere 改写后的 SQL。 */
    private boolean sqlShow;

    public List<Integer> getYears() {
        return years;
    }

    public void setYears(List<Integer> years) {
        this.years = years;
    }

    public int getOrderItemShardCount() {
        return orderItemShardCount;
    }

    public void setOrderItemShardCount(int orderItemShardCount) {
        this.orderItemShardCount = orderItemShardCount;
    }

    public boolean isSqlShow() {
        return sqlShow;
    }

    public void setSqlShow(boolean sqlShow) {
        this.sqlShow = sqlShow;
    }
}
