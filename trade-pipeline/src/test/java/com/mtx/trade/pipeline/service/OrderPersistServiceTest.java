package com.mtx.trade.pipeline.service;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.mtx.trade.common.id.GlobalIdGenerator;
import com.mtx.trade.common.id.IdGeneratorRegistry;
import com.mtx.trade.pipeline.config.FuiouOrderProperties;
import com.mtx.trade.pipeline.dto.OrderAggregate;
import com.mtx.trade.pipeline.entity.OrderDO;
import com.mtx.trade.pipeline.entity.OrderItemDO;
import com.mtx.trade.pipeline.enums.OrderPersistResult;
import com.mtx.trade.pipeline.service.db.OrderDbService;
import com.mtx.trade.pipeline.service.db.OrderItemDbService;
import com.mtx.trade.pipeline.service.db.OrderItemSpecDbService;
import com.mtx.trade.pipeline.service.db.OrderPackageItemDbService;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderPersistServiceTest {

    @Test
    void shouldNotExecuteRangeDeletesForNewOrder() {
        OrderDbService orderDbService = mock(OrderDbService.class);
        OrderItemDbService itemDbService = mock(OrderItemDbService.class);
        OrderItemSpecDbService specDbService = mock(OrderItemSpecDbService.class);
        OrderPackageItemDbService packageItemDbService = mock(OrderPackageItemDbService.class);
        IdGeneratorRegistry idGeneratorRegistry = mock(IdGeneratorRegistry.class);
        GlobalIdGenerator idGenerator = mock(GlobalIdGenerator.class);
        when(idGeneratorRegistry.global()).thenReturn(idGenerator);
        when(idGenerator.nextId()).thenReturn(101L, 102L);
        when(orderDbService.getOne(any(Wrapper.class), eq(false))).thenReturn(null);
        when(orderDbService.save(any(OrderDO.class))).thenReturn(true);
        when(itemDbService.saveBatch(any(Collection.class))).thenReturn(true);
        OrderPersistService service = new OrderPersistService(orderDbService, itemDbService,
                specDbService, packageItemDbService, idGeneratorRegistry, new FuiouOrderProperties());

        OrderDO order = new OrderDO();
        order.setOrderNo(6301055506L);
        order.setOrderCreateTime(LocalDateTime.of(2026, 7, 10, 6, 19, 16));
        order.setSourceUpdateTime(LocalDateTime.of(2026, 7, 10, 6, 19, 16));
        OrderItemDO item = new OrderItemDO();
        OrderAggregate aggregate = new OrderAggregate(order, List.of(item), List.of(), List.of(),
                order.getSourceUpdateTime().atZone(new FuiouOrderProperties().getZoneId()).toInstant().toEpochMilli());

        OrderPersistResult result = service.persist(aggregate);

        assertThat(result).isEqualTo(OrderPersistResult.APPLIED);
        assertThat(order.getId()).isEqualTo(101L);
        assertThat(item.getId()).isEqualTo(102L);
        verify(itemDbService, never()).list(any(Wrapper.class));
        verify(itemDbService, never()).remove(any(Wrapper.class));
        verify(specDbService, never()).list(any(Wrapper.class));
        verify(specDbService, never()).remove(any(Wrapper.class));
        verify(packageItemDbService, never()).list(any(Wrapper.class));
        verify(packageItemDbService, never()).remove(any(Wrapper.class));
    }
}
