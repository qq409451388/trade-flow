package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.pipeline.dto.OrderAggregate;
import com.mtx.trade.pipeline.dto.OrderEventMessage;
import com.mtx.trade.pipeline.entity.OrderDO;
import com.mtx.trade.pipeline.enums.OrderPersistResult;
import com.mtx.trade.pipeline.event.processor.FuiouOrderParser;
import com.mtx.trade.pipeline.event.processor.OrderEventHandler;
import com.mtx.trade.storage.api.StorageKey;
import com.mtx.trade.storage.api.StorageMetadata;
import com.mtx.trade.storage.api.StorageReader;
import org.junit.jupiter.api.Test;
import org.springframework.dao.PessimisticLockingFailureException;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderEventHandlerTest {

    @Test
    void shouldRetryWholeTransactionAfterDeadlock() {
        StorageReader storageReader = mock(StorageReader.class);
        FuiouOrderParser parser = mock(FuiouOrderParser.class);
        OrderPersistService persistService = mock(OrderPersistService.class);
        OrderEventHandler handler = new OrderEventHandler(storageReader, parser, persistService);
        byte[] sha256 = new byte[32];
        byte[] content = {1};
        OrderEventMessage event = new OrderEventMessage(1L, 2L, sha256, "order-1", 1,
                ContentType.ORDER.getCode(), 3L);
        StorageMetadata metadata = new StorageMetadata(2L, 1, ContentType.ORDER.getCode(),
                sha256, content.length, LocalDateTime.now());
        OrderDO order = new OrderDO();
        order.setOrderNo(100L);
        OrderAggregate aggregate = new OrderAggregate(order, List.of(), List.of(), List.of(), 3L);
        when(storageReader.getMetadata(any(StorageKey.class))).thenReturn(metadata);
        when(storageReader.getContent(any(StorageKey.class))).thenReturn(content);
        when(parser.parse(content, event)).thenReturn(aggregate);
        when(persistService.persist(aggregate))
                .thenThrow(new PessimisticLockingFailureException("deadlock"))
                .thenReturn(OrderPersistResult.APPLIED);

        OrderPersistResult result = handler.handle(event);

        assertThat(result).isEqualTo(OrderPersistResult.APPLIED);
        verify(persistService, times(2)).persist(aggregate);
    }
}
