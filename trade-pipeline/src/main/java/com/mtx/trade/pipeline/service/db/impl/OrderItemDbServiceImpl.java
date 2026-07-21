package com.mtx.trade.pipeline.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.pipeline.entity.OrderItemDO;
import com.mtx.trade.pipeline.mapper.OrderItemMapper;
import com.mtx.trade.pipeline.service.db.OrderItemDbService;
import org.springframework.stereotype.Service;

@Service
public class OrderItemDbServiceImpl extends ServiceImpl<OrderItemMapper, OrderItemDO>
        implements OrderItemDbService {
}
