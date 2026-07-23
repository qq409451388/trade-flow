package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.common.enums.DeliveryCircuitStatus;
import com.mtx.trade.ingress.config.EventDeliveryCircuitProperties;
import com.mtx.trade.ingress.entity.EventDeliveryControlDO;
import com.mtx.trade.ingress.service.db.EventDeliveryControlDbService;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

class EventDeliveryCircuitBreakerTest {
    @Test
    void shouldOpenAfterRedisFailureThreshold() {
        EventDeliveryControlDO state = state();
        EventDeliveryCircuitProperties properties = new EventDeliveryCircuitProperties();
        properties.setFailureThreshold(2);
        EventDeliveryCircuitBreaker breaker = new EventDeliveryCircuitBreaker(dbService(state), properties);

        breaker.recordPublishFailure(1, new IllegalStateException("redis down"));
        breaker.recordPublishFailure(1, new IllegalStateException("redis still down"));

        assertThat(state.getCircuitStatus()).isEqualTo(DeliveryCircuitStatus.OPEN.getCode());
        assertThat(state.getFailureCount()).isEqualTo(2);
    }

    private static EventDeliveryControlDO state() {
        EventDeliveryControlDO state = new EventDeliveryControlDO();
        state.setContentType(1);
        state.setCircuitStatus(DeliveryCircuitStatus.CLOSED.getCode());
        state.setFailureCount(0);
        state.setHealthSuccessCount(0);
        state.setVersion(0);
        return state;
    }

    private static EventDeliveryControlDbService dbService(EventDeliveryControlDO state) {
        return (EventDeliveryControlDbService) Proxy.newProxyInstance(
                EventDeliveryControlDbService.class.getClassLoader(),
                new Class<?>[]{EventDeliveryControlDbService.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getForUpdate", "getById" -> state;
                    case "updateById", "save" -> true;
                    case "toString" -> "EventDeliveryControlDbServiceStub";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
