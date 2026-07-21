package com.mtx.trade.pipeline.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.pipeline.entity.OrderEventProcessLogDO;
import com.mtx.trade.pipeline.mapper.OrderEventProcessLogMapper;
import com.mtx.trade.pipeline.service.db.OrderEventProcessLogDbService;
import org.springframework.stereotype.Service;

/** Pipeline 订单事件处理日志 DB Service 实现。 */
@Service
public class OrderEventProcessLogDbServiceImpl
        extends ServiceImpl<OrderEventProcessLogMapper, OrderEventProcessLogDO>
        implements OrderEventProcessLogDbService {
}
