package com.mtx.trade.ingress.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.ingress.entity.EventIngestFailureLogDO;
import com.mtx.trade.ingress.mapper.EventIngestFailureLogMapper;
import com.mtx.trade.ingress.service.db.EventIngestFailureLogDbService;
import org.springframework.stereotype.Service;

/** Event 接入失败审计 DB Service 实现。 */
@Service
public class EventIngestFailureLogDbServiceImpl
        extends ServiceImpl<EventIngestFailureLogMapper, EventIngestFailureLogDO>
        implements EventIngestFailureLogDbService {
}
