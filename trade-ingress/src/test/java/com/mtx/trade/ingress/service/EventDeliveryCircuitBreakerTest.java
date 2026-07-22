package com.mtx.trade.ingress.service;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import com.mtx.trade.ingress.common.enums.DeliveryCircuitStatus;
import com.mtx.trade.ingress.config.EventDeliveryCircuitProperties;
import com.mtx.trade.ingress.entity.EventDeliveryControlDO;
import com.mtx.trade.ingress.service.db.EventDeliveryControlDbService;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class EventDeliveryCircuitBreakerTest {

    @BeforeAll
    static void initializeMybatisMetadata() {
        TableInfoHelper.initTableInfo(
                new MapperBuilderAssistant(new MybatisConfiguration(), "test"), EventDeliveryControlDO.class);
    }

    @Test
    void shouldOpenAfterConsecutivePipelineFailures() {
        EventDeliveryCircuitProperties properties = new EventDeliveryCircuitProperties();
        properties.setPipelineFailureThreshold(2);
        properties.setHealthCheckDelay(Duration.ofSeconds(30));
        EventDeliveryControlDO control = new EventDeliveryControlDO();
        EventDeliveryCircuitBreaker breaker = new EventDeliveryCircuitBreaker(dbService(control), properties);
        initializeClosedControl(control, breaker);

        breaker.recordClosedPipelineFailure(1, new IllegalStateException("pipeline down"));

        assertThat(control.getCircuitStatus()).isEqualTo(DeliveryCircuitStatus.CLOSED.getCode());
        assertThat(control.getPipelineFailureCount()).isEqualTo(1);

        control.setRecoveryOwner(breaker.recoveryOwner());
        breaker.recordClosedPipelineFailure(1, new IllegalStateException("pipeline still down"));

        assertThat(control.getCircuitStatus()).isEqualTo(DeliveryCircuitStatus.OPEN.getCode());
        assertThat(control.getPipelineFailureCount()).isEqualTo(2);
        assertThat(control.getProbeEventId()).isNull();
    }

    @Test
    void shouldResetPipelineFailureCountAfterHealthyCheck() {
        EventDeliveryControlDO control = new EventDeliveryControlDO();
        EventDeliveryCircuitBreaker breaker =
                new EventDeliveryCircuitBreaker(dbService(control), new EventDeliveryCircuitProperties());
        initializeClosedControl(control, breaker);
        control.setPipelineFailureCount(1);

        breaker.recordClosedPipelineReady(1);

        assertThat(control.getPipelineFailureCount()).isZero();
        assertThat(control.getNextHealthCheckTime()).isNotNull();
        assertThat(control.getRecoveryOwner()).isNull();
    }

    private static void initializeClosedControl(
            EventDeliveryControlDO control, EventDeliveryCircuitBreaker breaker) {
        control.setContentType(1);
        control.setCircuitStatus(DeliveryCircuitStatus.CLOSED.getCode());
        control.setPipelineFailureCount(0);
        control.setFailureCount(0);
        control.setHealthSuccessCount(0);
        control.setRecoveryCursorId(0L);
        control.setRecoveryCutoffId(0L);
        control.setRecoveryOwner(breaker.recoveryOwner());
        control.setVersion(0);
    }

    private static EventDeliveryControlDbService dbService(EventDeliveryControlDO control) {
        return (EventDeliveryControlDbService) Proxy.newProxyInstance(
                EventDeliveryControlDbService.class.getClassLoader(),
                new Class<?>[]{EventDeliveryControlDbService.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getForUpdate", "getById" -> control;
                    case "update", "save" -> true;
                    case "toString" -> "EventDeliveryControlDbServiceStub";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
