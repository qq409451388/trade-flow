package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.common.enums.DeliveryCircuitStatus;
import com.mtx.trade.ingress.config.EventDeliveryCircuitProperties;
import com.mtx.trade.ingress.entity.EventDeliveryControlDO;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.client.RestClient;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EventDeliveryRecoveryServiceTest {

    @Test
    void shouldCountUnavailablePipelineWhileCircuitIsClosed() {
        RecordingCircuitBreaker breaker = new RecordingCircuitBreaker();
        breaker.closedControls = List.of(control(DeliveryCircuitStatus.CLOSED, null));
        StubReadinessClient readinessClient = new StubReadinessClient(false);
        EventDeliveryRecoveryService service = service(breaker, new StubEventDeliveryService(), readinessClient);

        service.recoverCircuitsAndDrainBacklog();

        assertThat(breaker.closedPipelineFailureRecorded).isTrue();
    }

    @Test
    void shouldCloseHalfOpenCircuitOnlyAfterProbeEventIsAcked() {
        RecordingCircuitBreaker breaker = new RecordingCircuitBreaker();
        EventDeliveryControlDO control = control(DeliveryCircuitStatus.HALF_OPEN, 99L);
        breaker.dueControls = List.of(control);
        breaker.control = control;
        StubEventDeliveryService deliveryService = new StubEventDeliveryService();
        deliveryService.acked = true;
        deliveryService.maxUnackedEventId = 120L;
        EventDeliveryRecoveryService service = service(
                breaker, deliveryService, new StubReadinessClient(true));

        service.recoverCircuitsAndDrainBacklog();

        assertThat(breaker.closedAfterProbeCutoff).isEqualTo(120L);
    }

    @Test
    void shouldKeepHalfOpenWhileProbeEventIsUnacked() {
        RecordingCircuitBreaker breaker = new RecordingCircuitBreaker();
        EventDeliveryControlDO control = control(DeliveryCircuitStatus.HALF_OPEN, 99L);
        breaker.dueControls = List.of(control);
        breaker.control = control;
        StubEventDeliveryService deliveryService = new StubEventDeliveryService();
        EventDeliveryRecoveryService service = service(
                breaker, deliveryService, new StubReadinessClient(true));

        service.recoverCircuitsAndDrainBacklog();

        assertThat(breaker.waitedProbeEventId).isEqualTo(99L);
        assertThat(breaker.closedAfterProbeCutoff).isNull();
    }

    private static EventDeliveryRecoveryService service(
            RecordingCircuitBreaker breaker,
            StubEventDeliveryService deliveryService,
            StubReadinessClient readinessClient) {
        return new EventDeliveryRecoveryService(
                breaker,
                deliveryService,
                readinessClient,
                new EventDeliveryCircuitProperties(),
                new StringRedisTemplate());
    }

    private static EventDeliveryControlDO control(DeliveryCircuitStatus status, Long probeEventId) {
        EventDeliveryControlDO control = new EventDeliveryControlDO();
        control.setContentType(1);
        control.setCircuitStatus(status.getCode());
        control.setProbeEventId(probeEventId);
        return control;
    }

    private static final class RecordingCircuitBreaker extends EventDeliveryCircuitBreaker {
        private List<EventDeliveryControlDO> closedControls = List.of();
        private List<EventDeliveryControlDO> dueControls = List.of();
        private EventDeliveryControlDO control;
        private boolean closedPipelineFailureRecorded;
        private Long closedAfterProbeCutoff;
        private Long waitedProbeEventId;

        private RecordingCircuitBreaker() {
            super(null, new EventDeliveryCircuitProperties());
        }

        @Override
        public List<EventDeliveryControlDO> listClosedForHealthCheck() {
            return closedControls;
        }

        @Override
        public List<EventDeliveryControlDO> listDueForHealthCheck() {
            return dueControls;
        }

        @Override
        public List<EventDeliveryControlDO> listDrainable() {
            return List.of();
        }

        @Override
        public boolean claim(int contentType, int requiredStatus) {
            return true;
        }

        @Override
        public void recordClosedPipelineFailure(int contentType, Throwable failure) {
            closedPipelineFailureRecorded = true;
        }

        @Override
        public EventDeliveryControlDO getControl(int contentType) {
            return control;
        }

        @Override
        public void closeAfterProbe(int contentType, long recoveryCutoffId) {
            closedAfterProbeCutoff = recoveryCutoffId;
        }

        @Override
        public void waitForProbeAck(int contentType, Long probeEventId) {
            waitedProbeEventId = probeEventId;
        }
    }

    private static final class StubEventDeliveryService extends EventDeliveryService {
        private boolean acked;
        private long maxUnackedEventId;

        private StubEventDeliveryService() {
            super(null, null, null, null, null, null, null);
        }

        @Override
        public boolean isEventAcked(int contentType, long eventId) {
            return acked;
        }

        @Override
        public long maxUnackedEventId(int contentType) {
            return maxUnackedEventId;
        }
    }

    private static final class StubReadinessClient extends PipelineReadinessClient {
        private final boolean ready;

        private StubReadinessClient(boolean ready) {
            super(RestClient.builder(), new EventDeliveryCircuitProperties());
            this.ready = ready;
        }

        @Override
        public boolean isReady(int contentType) {
            return ready;
        }
    }
}
