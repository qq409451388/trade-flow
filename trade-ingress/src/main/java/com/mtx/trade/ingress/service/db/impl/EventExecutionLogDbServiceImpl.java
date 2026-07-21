package com.mtx.trade.ingress.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.ingress.entity.EventExecutionLogDO;
import com.mtx.trade.ingress.mapper.EventExecutionLogMapper;
import com.mtx.trade.ingress.service.db.EventExecutionLogDbService;
import org.springframework.stereotype.Service;

/**
 * 事件执行流水 DB Service 实现。
 */
@Service
public class EventExecutionLogDbServiceImpl
        extends ServiceImpl<EventExecutionLogMapper, EventExecutionLogDO>
        implements EventExecutionLogDbService {
}
