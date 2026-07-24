package com.mtx.trade.pipeline.service.impl;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.pipeline.dto.OrderEventMessage;
import com.mtx.trade.pipeline.dto.PaymentEventMessage;
import com.mtx.trade.pipeline.entity.OrderEventProcessLogDO;
import com.mtx.trade.pipeline.entity.PaymentEventProcessLogDO;
import com.mtx.trade.pipeline.service.OrderEventProcessLogService;
import com.mtx.trade.pipeline.enums.OrderPersistResult;
import com.mtx.trade.pipeline.service.PaymentEventProcessLogService;
import com.mtx.trade.pipeline.enums.PaymentPersistResult;
import com.mtx.trade.pipeline.service.db.OrderEventProcessLogDbService;
import com.mtx.trade.pipeline.service.db.PaymentEventProcessLogDbService;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventProcessLogServiceImplTest {

    @Test
    void shouldTrackOrderAckAndXackLifecycle() {
        List<OrderEventProcessLogDO> saved = new ArrayList<>();
        List<OrderEventProcessLogDO> updated = new ArrayList<>();
        OrderEventProcessLogDbService dbService = stub(
                OrderEventProcessLogDbService.class, saved, updated, 11L);
        OrderEventProcessLogServiceImpl service = new OrderEventProcessLogServiceImpl(dbService);

        long logId = service.recordSuccess(orderEvent(), "1-0", OrderEventProcessLogService.TRIGGER_STREAM,
                OrderPersistResult.APPLIED, LocalDateTime.now(), System.nanoTime());
        service.recordIngressAck(logId, true);
        service.recordRedisXack(logId, false);

        assertThat(logId).isEqualTo(11L);
        assertThat(saved.get(0).getIngressAckStatus()).isEqualTo(OrderEventProcessLogService.DELIVERY_PENDING);
        assertThat(saved.get(0).getRedisXackStatus()).isEqualTo(OrderEventProcessLogService.DELIVERY_PENDING);
        assertThat(updated.get(0).getIngressAckStatus())
                .isEqualTo(OrderEventProcessLogService.DELIVERY_SUCCEEDED);
        assertThat(updated.get(0).getIngressAckTime()).isNotNull();
        assertThat(updated.get(1).getRedisXackStatus())
                .isEqualTo(OrderEventProcessLogService.DELIVERY_FAILED);
    }

    @Test
    void shouldMarkRedisXackNotApplicableForActivelyPulledPayment() {
        List<PaymentEventProcessLogDO> saved = new ArrayList<>();
        PaymentEventProcessLogDbService dbService = stub(
                PaymentEventProcessLogDbService.class, saved, new ArrayList<>(), 12L);
        PaymentEventProcessLogServiceImpl service = new PaymentEventProcessLogServiceImpl(dbService);

        long logId = service.recordSuccess(paymentEvent(), null,
                PaymentEventProcessLogService.TRIGGER_ACTIVE_PULL, PaymentPersistResult.IGNORED_DUPLICATE,
                LocalDateTime.now(), System.nanoTime());

        assertThat(logId).isEqualTo(12L);
        assertThat(saved.get(0).getRedisXackStatus())
                .isEqualTo(PaymentEventProcessLogService.DELIVERY_NOT_APPLICABLE);
    }

    @Test
    void shouldTreatSuccessfulAndTerminalAuditsAsAckOnly() {
        OrderEventProcessLogDbService dbService = mock(OrderEventProcessLogDbService.class);
        when(dbService.list(any(Wrapper.class))).thenReturn(List.of(
                orderLog(1L, OrderEventProcessLogService.STATUS_APPLIED,
                        OrderEventProcessLogService.TRIGGER_STREAM),
                orderLog(2L, OrderEventProcessLogService.STATUS_FAILED,
                        OrderEventProcessLogService.TRIGGER_ACTIVE_PULL),
                orderLog(2L, OrderEventProcessLogService.STATUS_FAILED,
                        OrderEventProcessLogService.TRIGGER_ACTIVE_PULL),
                orderLog(3L, OrderEventProcessLogService.STATUS_FAILED,
                        OrderEventProcessLogService.TRIGGER_ACTIVE_PULL)));
        OrderEventProcessLogServiceImpl service = new OrderEventProcessLogServiceImpl(dbService);

        Set<Long> ackOnly = service.findAckOnlyEventIds(List.of(1L, 2L, 3L), 2);

        assertThat(ackOnly).containsExactlyInAnyOrder(1L, 2L);
    }

    @SuppressWarnings("unchecked")
    private static <S, E> S stub(Class<S> serviceType, List<E> saved, List<E> updated, long id) {
        return (S) Proxy.newProxyInstance(serviceType.getClassLoader(), new Class<?>[]{serviceType},
                (proxy, method, args) -> {
                    if ("save".equals(method.getName())) {
                        E entity = (E) args[0];
                        if (entity instanceof OrderEventProcessLogDO order) {
                            order.setId(id);
                        } else if (entity instanceof PaymentEventProcessLogDO payment) {
                            payment.setId(id);
                        }
                        saved.add(entity);
                        return true;
                    }
                    if ("updateById".equals(method.getName())) {
                        updated.add((E) args[0]);
                        return true;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static OrderEventMessage orderEvent() {
        return new OrderEventMessage(1L, 2L, new byte[32], "order-1", 1,
                ContentType.ORDER.getCode(), 3L);
    }

    private static PaymentEventMessage paymentEvent() {
        return new PaymentEventMessage(1L, 2L, new byte[32], "payment-1", 1,
                ContentType.PAYMENT.getCode(), 3L);
    }

    private static OrderEventProcessLogDO orderLog(long eventId, int status, int triggerType) {
        OrderEventProcessLogDO log = new OrderEventProcessLogDO();
        log.setEventId(eventId);
        log.setProcessStatus(status);
        log.setTriggerType(triggerType);
        return log;
    }
}
