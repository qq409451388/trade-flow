package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.utils.EnterpriseWechatRobotUtils;
import com.mtx.trade.pipeline.config.UnackedEventPullProperties;
import com.mtx.trade.pipeline.dto.OrderEventPullResult;
import com.mtx.trade.pipeline.task.UnackedEventPullScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UnackedEventPullSchedulerTest {

    @Mock
    private EventPullLeaseService leaseService;
    @Mock
    private OrderEventPullService orderEventPullService;
    @Mock
    private PaymentEventPullService paymentEventPullService;
    @Mock
    private EnterpriseWechatRobotUtils enterpriseWechatRobotUtils;

    private UnackedEventPullScheduler scheduler;

    @BeforeEach
    void setUp() {
        UnackedEventPullProperties properties = new UnackedEventPullProperties();
        properties.setBatchSize(100);
        scheduler = new UnackedEventPullScheduler(
                properties, leaseService, orderEventPullService, paymentEventPullService,
                enterpriseWechatRobotUtils);
    }

    @Test
    void shouldPullAndReleaseWhenOrderLeaseAcquired() {
        int contentType = ContentType.ORDER.getCode();
        when(leaseService.tryAcquire(contentType)).thenReturn(true);
        when(orderEventPullService.pull(any())).thenReturn(List.of(
                new OrderEventPullResult(1L, "APPLIED", "processed")));

        scheduler.pullOrders();

        verify(orderEventPullService).pull(any());
        verify(leaseService).release(contentType);
    }

    @Test
    void shouldSkipOrderPullWhenLeaseUnavailable() {
        int contentType = ContentType.ORDER.getCode();
        when(leaseService.tryAcquire(contentType)).thenReturn(false);

        scheduler.pullOrders();

        verify(orderEventPullService, never()).pull(any());
        verify(leaseService, never()).release(contentType);
    }

    @Test
    void shouldReleaseLeaseWhenOrderPullFails() {
        int contentType = ContentType.ORDER.getCode();
        when(leaseService.tryAcquire(contentType)).thenReturn(true);
        when(orderEventPullService.pull(any())).thenThrow(new IllegalStateException("test"));

        scheduler.pullOrders();

        verify(leaseService).release(contentType);
    }
}
