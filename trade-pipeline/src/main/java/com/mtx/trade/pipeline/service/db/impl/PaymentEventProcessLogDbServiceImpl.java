package com.mtx.trade.pipeline.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.pipeline.entity.PaymentEventProcessLogDO;
import com.mtx.trade.pipeline.mapper.PaymentEventProcessLogMapper;
import com.mtx.trade.pipeline.service.db.PaymentEventProcessLogDbService;
import org.springframework.stereotype.Service;

@Service
public class PaymentEventProcessLogDbServiceImpl
        extends ServiceImpl<PaymentEventProcessLogMapper, PaymentEventProcessLogDO>
        implements PaymentEventProcessLogDbService {
}
