package com.mtx.trade.receiver.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.receiver.entity.OrderEventDO;
import com.mtx.trade.receiver.mapper.OrderEventMapper;
import com.mtx.trade.receiver.service.db.OrderEventDbService;
import org.springframework.stereotype.Service;

@Service
public class OrderEventDbServiceImpl extends ServiceImpl<OrderEventMapper, OrderEventDO> implements OrderEventDbService {
}
