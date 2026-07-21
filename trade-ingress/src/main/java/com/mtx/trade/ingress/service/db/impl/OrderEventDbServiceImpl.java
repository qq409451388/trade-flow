package com.mtx.trade.ingress.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.ingress.entity.OrderEventDO;
import com.mtx.trade.ingress.mapper.OrderEventMapper;
import com.mtx.trade.ingress.service.db.OrderEventDbService;
import org.springframework.stereotype.Service;

@Service
public class OrderEventDbServiceImpl extends ServiceImpl<OrderEventMapper, OrderEventDO> implements OrderEventDbService {
}
