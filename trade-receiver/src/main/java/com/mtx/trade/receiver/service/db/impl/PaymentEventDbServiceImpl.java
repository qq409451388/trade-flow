package com.mtx.trade.receiver.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.receiver.entity.PaymentEventDO;
import com.mtx.trade.receiver.mapper.PaymentEventMapper;
import com.mtx.trade.receiver.service.db.PaymentEventDbService;
import org.springframework.stereotype.Service;

@Service
public class PaymentEventDbServiceImpl extends ServiceImpl<PaymentEventMapper, PaymentEventDO> implements PaymentEventDbService {
}
