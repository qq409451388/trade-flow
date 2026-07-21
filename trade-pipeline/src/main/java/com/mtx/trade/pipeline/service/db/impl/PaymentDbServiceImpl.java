package com.mtx.trade.pipeline.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.pipeline.entity.PaymentDO;
import com.mtx.trade.pipeline.mapper.PaymentMapper;
import com.mtx.trade.pipeline.service.db.PaymentDbService;
import org.springframework.stereotype.Service;

@Service
public class PaymentDbServiceImpl extends ServiceImpl<PaymentMapper, PaymentDO> implements PaymentDbService {
}
