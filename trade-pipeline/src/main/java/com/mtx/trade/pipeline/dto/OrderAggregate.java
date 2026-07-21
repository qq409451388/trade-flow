package com.mtx.trade.pipeline.dto;

import com.mtx.trade.pipeline.entity.OrderDO;
import com.mtx.trade.pipeline.entity.OrderItemDO;
import com.mtx.trade.pipeline.entity.OrderItemSpecDO;
import com.mtx.trade.pipeline.entity.OrderPackageItemDO;

import java.util.List;

/** 一次富友订单事件解析出的完整订单快照。 */
public record OrderAggregate(
        OrderDO order,
        List<OrderItemDO> items,
        List<OrderItemSpecDO> specs,
        List<OrderPackageItemDO> packageItems,
        long messageVersion) {

    public OrderAggregate {
        items = List.copyOf(items);
        specs = List.copyOf(specs);
        packageItems = List.copyOf(packageItems);
    }
}
