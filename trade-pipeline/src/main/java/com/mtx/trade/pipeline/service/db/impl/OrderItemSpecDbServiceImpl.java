package com.mtx.trade.pipeline.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.pipeline.entity.OrderItemSpecDO;
import com.mtx.trade.pipeline.mapper.OrderItemSpecMapper;
import com.mtx.trade.pipeline.service.db.OrderItemSpecDbService;
import org.springframework.stereotype.Service;

@Service
public class OrderItemSpecDbServiceImpl extends ServiceImpl<OrderItemSpecMapper, OrderItemSpecDO>
        implements OrderItemSpecDbService {
}
