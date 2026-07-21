package com.mtx.trade.receiver.service.impl;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import com.mtx.trade.receiver.dto.EventIngestResult;
import com.mtx.trade.receiver.entity.OrderEventDO;
import com.mtx.trade.receiver.service.db.OrderEventDbService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.springframework.dao.DuplicateKeyException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrderEventServiceImplTest {

    private final OrderEventDbService dbService = mock(OrderEventDbService.class);
    private final OrderEventServiceImpl service = new OrderEventServiceImpl(dbService);
    private final byte[] sha256 = new byte[32];

    @BeforeAll
    static void initializeMybatisMetadata() {
        TableInfoHelper.initTableInfo(
                new MapperBuilderAssistant(new MybatisConfiguration(), "test"), OrderEventDO.class);
    }

    @Test
    void shouldInsertEveryNewVersionInAllVersionsMode() {
        when(dbService.save(any(OrderEventDO.class))).thenReturn(true);

        EventIngestResult<OrderEventDO> result = service.createEvent(1, "O-100", 2L, 100L, sha256);

        assertThat(result.accepted()).isTrue();
        assertThat(result.event().getMessageVersion()).isEqualTo(2L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldNotAcceptDuplicateBusinessVersion() {
        OrderEventDO existing = new OrderEventDO();
        existing.setId(1L);
        existing.setMessageVersion(2L);
        when(dbService.save(any(OrderEventDO.class))).thenThrow(new DuplicateKeyException("duplicate"));
        when(dbService.getOne(any(Wrapper.class), anyBoolean())).thenReturn(existing);

        EventIngestResult<OrderEventDO> result = service.createEvent(1, "O-100", 2L, 100L, sha256);

        assertThat(result.accepted()).isFalse();
        assertThat(result.event()).isSameAs(existing);
    }

}
