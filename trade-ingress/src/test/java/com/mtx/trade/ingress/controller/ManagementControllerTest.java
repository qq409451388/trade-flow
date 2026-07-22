package com.mtx.trade.ingress.controller;

import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.dto.EventIngestResult;
import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.service.OrderEventService;
import com.mtx.trade.storage.api.StorageKey;
import com.mtx.trade.storage.api.StorageMetadata;
import com.mtx.trade.storage.api.StorageReader;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ManagementControllerTest {

    @Test
    void shouldReturnUtf8ContentInStandardResponse() {
        OrderEventDO event = new OrderEventDO();
        event.setRawId(2L);
        event.setPayloadSha256(new byte[32]);
        ManagementController controller = new ManagementController(
                storageReader("中文报文".getBytes(StandardCharsets.UTF_8)), orderService(event));

        var response = controller.getBlob(1L);

        assertThat(response.getCode()).isZero();
        assertThat(response.getData()).isEqualTo("中文报文");
    }

    @Test
    void shouldRejectMissingOrderEventInsteadOfThrowingNpe() {
        ManagementController controller = new ManagementController(storageReader(new byte[0]), orderService(null));

        assertThatThrownBy(() -> controller.getBlob(1L))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("订单事件不存在");
    }

    private static StorageReader storageReader(byte[] content) {
        return new StorageReader() {
            @Override
            public StorageMetadata getMetadata(StorageKey key) {
                return null;
            }

            @Override
            public byte[] getContent(StorageKey key) {
                return content;
            }
        };
    }

    private static OrderEventService orderService(OrderEventDO event) {
        return new OrderEventService() {
            @Override
            public EventIngestResult<OrderEventDO> createEvent(
                    int sourceSystem,
                    String thirdEventKey,
                    long messageVersion,
                    Long rawId,
                    byte[] payloadSha256) {
                throw new UnsupportedOperationException();
            }

            @Override
            public OrderEventDO getById(Long id) {
                return event;
            }
        };
    }
}
