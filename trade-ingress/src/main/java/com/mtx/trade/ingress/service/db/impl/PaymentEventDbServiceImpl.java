package com.mtx.trade.ingress.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.ingress.entity.PaymentEventDO;
import com.mtx.trade.ingress.mapper.PaymentEventMapper;
import com.mtx.trade.ingress.service.db.PaymentEventDbService;
import org.springframework.stereotype.Service;

@Service
public class PaymentEventDbServiceImpl extends ServiceImpl<PaymentEventMapper, PaymentEventDO> implements PaymentEventDbService {
}
