package com.mtx.trade.receiver.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.receiver.entity.EventExecutionLogDO;
import com.mtx.trade.receiver.mapper.EventExecutionLogMapper;
import com.mtx.trade.receiver.service.db.EventExecutionLogDbService;
import org.springframework.stereotype.Service;

/**
 * 事件执行流水 DB Service 实现。
 */
@Service
public class EventExecutionLogDbServiceImpl
        extends ServiceImpl<EventExecutionLogMapper, EventExecutionLogDO>
        implements EventExecutionLogDbService {
}
