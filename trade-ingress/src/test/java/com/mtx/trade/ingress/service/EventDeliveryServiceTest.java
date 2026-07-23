package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.common.enums.EventAckStatus;
import com.mtx.trade.ingress.config.EventDeliveryProperties;
import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.service.db.OrderEventDbService;
import com.mtx.trade.ingress.service.db.PaymentEventDbService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.scheduling.TaskScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EventDeliveryServiceTest {

    private EventStreamPublisher publisher;
    private OrderEventDbService orderEventDbService;
    private TaskScheduler retryScheduler;
    private EventDeliveryCircuitBreaker circuitBreaker;
    private EventDeliveryService service;

    @BeforeEach
    void setUp() {
        publisher = mock(EventStreamPublisher.class);
        orderEventDbService = mock(OrderEventDbService.class);
        retryScheduler = mock(TaskScheduler.class);
        circuitBreaker = mock(EventDeliveryCircuitBreaker.class);
        EventDeliveryProperties properties = new EventDeliveryProperties();
        properties.setRetryDelays(List.of(Duration.ZERO, Duration.ZERO));
        service = new EventDeliveryService(publisher, orderEventDbService,
                mock(PaymentEventDbService.class), retryScheduler, properties, circuitBreaker);
    }

    @Test
    void shouldNotScheduleRetryWhenInitialPublishSucceeds() {
        OrderEventDO event = event();
        when(circuitBreaker.allowPublish(1)).thenReturn(true);

        service.deliverOrderEvent(event);

        verify(publisher).publishOrderEvent(event);
        verify(retryScheduler, never()).schedule(any(Runnable.class), any(Instant.class));
    }

    @Test
    void shouldStopRetryingAsSoonAsPublishSucceeds() {
        OrderEventDO event = event();
        when(circuitBreaker.allowPublish(1)).thenReturn(true);
        when(orderEventDbService.getById(event.getId())).thenReturn(event);
        doThrow(new IllegalStateException("redis unavailable"))
                .doNothing().when(publisher).publishOrderEvent(event);

        service.deliverOrderEvent(event);
        ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);
        verify(retryScheduler).schedule(retry.capture(), any(Instant.class));

        retry.getValue().run();

        verify(publisher, times(2)).publishOrderEvent(event);
        verify(retryScheduler, times(1)).schedule(any(Runnable.class), any(Instant.class));
    }

    private static OrderEventDO event() {
        OrderEventDO event = new OrderEventDO();
        event.setId(100L);
        event.setAcked(EventAckStatus.INIT.getCode());
        return event;
    }
}
