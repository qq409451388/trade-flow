package com.mtx.trade.pipeline.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.common.id.IdGeneratorRegistry;
import com.mtx.trade.pipeline.config.FuiouOrderProperties;
import com.mtx.trade.pipeline.config.OrderItemSpecShardingHint;
import com.mtx.trade.pipeline.dto.OrderAggregate;
import com.mtx.trade.pipeline.entity.OrderDO;
import com.mtx.trade.pipeline.entity.OrderItemDO;
import com.mtx.trade.pipeline.entity.OrderItemSpecDO;
import com.mtx.trade.pipeline.entity.OrderPackageItemDO;
import com.mtx.trade.pipeline.enums.OrderPersistResult;
import com.mtx.trade.pipeline.exception.ConcurrentOrderInsertException;
import com.mtx.trade.pipeline.service.db.OrderDbService;
import com.mtx.trade.pipeline.service.db.OrderItemDbService;
import com.mtx.trade.pipeline.service.db.OrderItemSpecDbService;
import com.mtx.trade.pipeline.service.db.OrderPackageItemDbService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.hint.HintManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.dao.DuplicateKeyException;

import java.time.LocalDateTime;
import java.util.List;

/** 订单主快照及全部子表的单事件事务落库。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderPersistService {

    private final OrderDbService orderDbService;
    private final OrderItemDbService orderItemDbService;
    private final OrderItemSpecDbService orderItemSpecDbService;
    private final OrderPackageItemDbService orderPackageItemDbService;
    private final IdGeneratorRegistry idGeneratorRegistry;
    private final FuiouOrderProperties fuiouOrderProperties;

    @Transactional(transactionManager = "pipelineTransactionManager", rollbackFor = Exception.class)
    public OrderPersistResult persist(OrderAggregate aggregate) {
        OrderDO incoming = aggregate.order();
        LocalDateTime yearStart = incoming.getOrderCreateTime().withDayOfYear(1).toLocalDate().atStartOfDay();
        LocalDateTime nextYearStart = yearStart.plusYears(1);
        boolean replaceExistingChildren;

        OrderDO existing = orderDbService.getOne(new LambdaQueryWrapper<OrderDO>()
                .eq(OrderDO::getOrderNo, incoming.getOrderNo())
                .ge(OrderDO::getOrderCreateTime, yearStart)
                .lt(OrderDO::getOrderCreateTime, nextYearStart), false);

        if (existing == null) {
            replaceExistingChildren = false;
            incoming.setId(nextId());
            try {
                if (!orderDbService.save(incoming)) {
                    throw dataError("订单主表新增失败: orderNo=" + incoming.getOrderNo());
                }
            } catch (DuplicateKeyException e) {
                throw new ConcurrentOrderInsertException(e);
            }
        } else {
            replaceExistingChildren = true;
            long existingVersion = versionOf(existing.getSourceUpdateTime());
            if (aggregate.messageVersion() <= existingVersion) {
                log.debug("[Order Persistence] ✅ Duplicate or stale event was ignored. "
                                + "orderNo={}, incomingVersion={}, existingVersion={}",
                        incoming.getOrderNo(), aggregate.messageVersion(), existingVersion);
                return OrderPersistResult.IGNORED_DUPLICATE_OR_STALE;
            }
            incoming.setId(existing.getId());
            boolean updated = orderDbService.update(incoming, new LambdaUpdateWrapper<OrderDO>()
                    .eq(OrderDO::getId, existing.getId())
                    .ge(OrderDO::getOrderCreateTime, yearStart)
                    .lt(OrderDO::getOrderCreateTime, nextYearStart)
                    .and(wrapper -> wrapper
                            .isNull(OrderDO::getSourceUpdateTime)
                            .or()
                            .lt(OrderDO::getSourceUpdateTime, incoming.getSourceUpdateTime())));
            if (!updated) {
                log.debug("[Order Persistence] 🔄 Version race was lost; the older event was ignored. "
                                + "orderNo={}, incomingVersion={}",
                        incoming.getOrderNo(), aggregate.messageVersion());
                return OrderPersistResult.IGNORED_DUPLICATE_OR_STALE;
            }
        }

        replaceChildren(aggregate, yearStart, nextYearStart, replaceExistingChildren);
        log.debug("[Order Persistence] ✅ Order snapshot persisted. "
                        + "orderNo={}, version={}, items={}, specs={}, packageItems={}",
                incoming.getOrderNo(), aggregate.messageVersion(), aggregate.items().size(),
                aggregate.specs().size(), aggregate.packageItems().size());
        return OrderPersistResult.APPLIED;
    }

    private void replaceChildren(
            OrderAggregate aggregate,
            LocalDateTime yearStart,
            LocalDateTime nextYearStart,
            boolean replaceExistingChildren) {
        Long orderNo = aggregate.order().getOrderNo();
        int orderYear = aggregate.order().getOrderCreateTime().getYear();
        if (replaceExistingChildren) {
            removeExistingChildren(orderNo, orderYear, yearStart, nextYearStart);
        }

        aggregate.items().forEach(item -> item.setId(nextId()));
        aggregate.specs().forEach(spec -> spec.setId(nextId()));
        aggregate.packageItems().forEach(packageItem -> packageItem.setId(nextId()));

        if (!aggregate.items().isEmpty() && !orderItemDbService.saveBatch(aggregate.items())) {
            throw dataError("订单商品明细批量新增失败: orderNo=" + orderNo);
        }
        if (!aggregate.specs().isEmpty()) {
            try (HintManager ignored = OrderItemSpecShardingHint.useYear(orderYear)) {
                if (!orderItemSpecDbService.saveBatch(aggregate.specs())) {
                    throw dataError("订单规格明细批量新增失败: orderNo=" + orderNo);
                }
            }
        }
        if (!aggregate.packageItems().isEmpty()
                && !orderPackageItemDbService.saveBatch(aggregate.packageItems())) {
            throw dataError("订单套餐子商品批量新增失败: orderNo=" + orderNo);
        }
    }

    /**
     * 先用普通快照查询解析旧记录，再携带主键集合和分片条件精确删除。
     * 禁止对可能为空的 order_no 范围直接 DELETE；REPEATABLE READ 下该写法会持有 gap lock，
     * 让同一物理分片内不同订单的后续 INSERT 形成死锁。
     */
    private void removeExistingChildren(
            Long orderNo,
            int orderYear,
            LocalDateTime yearStart,
            LocalDateTime nextYearStart) {
        List<OrderItemDO> oldItems = orderItemDbService.list(new LambdaQueryWrapper<OrderItemDO>()
                .eq(OrderItemDO::getOrderNo, orderNo)
                .ge(OrderItemDO::getItemCreateTime, yearStart)
                .lt(OrderItemDO::getItemCreateTime, nextYearStart));
        List<Long> oldItemIds = oldItems.stream()
                .map(OrderItemDO::getId)
                .filter(java.util.Objects::nonNull)
                .toList();
        List<Long> oldDetailNos = oldItems.stream()
                .map(OrderItemDO::getDetailNo)
                .filter(java.util.Objects::nonNull)
                .toList();

        try (HintManager ignored = OrderItemSpecShardingHint.useYear(orderYear)) {
            if (!oldDetailNos.isEmpty()) {
                List<Long> oldSpecIds = orderItemSpecDbService.list(
                                new LambdaQueryWrapper<OrderItemSpecDO>()
                                        .select(OrderItemSpecDO::getId)
                                        .in(OrderItemSpecDO::getDetailNo, oldDetailNos))
                        .stream()
                        .map(OrderItemSpecDO::getId)
                        .filter(java.util.Objects::nonNull)
                        .toList();
                if (!oldSpecIds.isEmpty() && !orderItemSpecDbService.remove(
                        new LambdaQueryWrapper<OrderItemSpecDO>().in(OrderItemSpecDO::getId, oldSpecIds))) {
                    throw dataError("订单规格明细删除失败: orderNo=" + orderNo);
                }
            }
        }

        if (!oldItemIds.isEmpty() && !orderItemDbService.remove(new LambdaQueryWrapper<OrderItemDO>()
                .in(OrderItemDO::getId, oldItemIds)
                .eq(OrderItemDO::getOrderNo, orderNo)
                .ge(OrderItemDO::getItemCreateTime, yearStart)
                .lt(OrderItemDO::getItemCreateTime, nextYearStart))) {
            throw dataError("订单商品明细删除失败: orderNo=" + orderNo);
        }

        List<Long> oldPackageItemIds = orderPackageItemDbService.list(
                        new LambdaQueryWrapper<OrderPackageItemDO>()
                                .select(OrderPackageItemDO::getId)
                                .eq(OrderPackageItemDO::getOrderNo, orderNo)
                                .ge(OrderPackageItemDO::getItemCreateTime, yearStart)
                                .lt(OrderPackageItemDO::getItemCreateTime, nextYearStart))
                .stream()
                .map(OrderPackageItemDO::getId)
                .filter(java.util.Objects::nonNull)
                .toList();
        if (!oldPackageItemIds.isEmpty() && !orderPackageItemDbService.remove(
                new LambdaQueryWrapper<OrderPackageItemDO>()
                        .in(OrderPackageItemDO::getId, oldPackageItemIds)
                        .eq(OrderPackageItemDO::getOrderNo, orderNo)
                        .ge(OrderPackageItemDO::getItemCreateTime, yearStart)
                        .lt(OrderPackageItemDO::getItemCreateTime, nextYearStart))) {
            throw dataError("订单套餐子商品删除失败: orderNo=" + orderNo);
        }
    }

    private long nextId() {
        return idGeneratorRegistry.global().nextId();
    }

    private long versionOf(LocalDateTime sourceUpdateTime) {
        if (sourceUpdateTime == null) {
            return Long.MIN_VALUE;
        }
        return sourceUpdateTime.atZone(fuiouOrderProperties.getZoneId()).toInstant().toEpochMilli();
    }

    private static BusinessException dataError(String message) {
        return new BusinessException(ErrorCode.DATA_CREATE_ERROR, message);
    }
}
