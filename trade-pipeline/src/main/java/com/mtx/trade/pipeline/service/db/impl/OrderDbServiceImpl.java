package com.mtx.trade.pipeline.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.pipeline.entity.OrderDO;
import com.mtx.trade.pipeline.mapper.OrderMapper;
import com.mtx.trade.pipeline.service.db.OrderDbService;
import org.springframework.stereotype.Service;

@Service
public class OrderDbServiceImpl extends ServiceImpl<OrderMapper, OrderDO> implements OrderDbService {
}
