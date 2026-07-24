package com.mtx.trade.ingress.service.impl;

import com.mtx.trade.ingress.entity.EventIngestFailureLogDO;
import com.mtx.trade.ingress.service.EventIngestFailureLogService;
import com.mtx.trade.ingress.service.db.EventIngestFailureLogDbService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EventIngestFailureLogServiceImplTest {

    @Test
    void shouldAuditStorageFailureWithoutStorageIdAndKeepCauseChain() {
        EventIngestFailureLogDbService dbService = mock(EventIngestFailureLogDbService.class);
        when(dbService.save(any())).thenAnswer(invocation -> {
            EventIngestFailureLogDO entity = invocation.getArgument(0);
            entity.setId(42L);
            return true;
        });
        EventIngestFailureLogServiceImpl service = new EventIngestFailureLogServiceImpl(dbService);
        byte[] sha256 = new byte[32];
        RuntimeException failure = new RuntimeException(
                "storage persistence failed", new IllegalStateException("connection reset"));

        Long auditId = service.recordFailure(
                "0123456789abcdef0123456789abcdef",
                1,
                1,
                sha256,
                null,
                EventIngestFailureLogService.STAGE_STORAGE_PERSIST,
                null,
                failure);

        ArgumentCaptor<EventIngestFailureLogDO> captor =
                ArgumentCaptor.forClass(EventIngestFailureLogDO.class);
        verify(dbService).save(captor.capture());
        EventIngestFailureLogDO saved = captor.getValue();
        assertThat(auditId).isEqualTo(42L);
        assertThat(saved.getRequestId()).isEqualTo("0123456789abcdef0123456789abcdef");
        assertThat(saved.getRawId()).isNull();
        assertThat(saved.getPayloadSha256()).isSameAs(sha256);
        assertThat(saved.getFailureStage()).isEqualTo("STORAGE_PERSIST");
        assertThat(saved.getExceptionType()).isEqualTo(RuntimeException.class.getName());
        assertThat(saved.getFailureReason())
                .isEqualTo("RuntimeException: storage persistence failed"
                        + " <- IllegalStateException: connection reset");
    }
}
