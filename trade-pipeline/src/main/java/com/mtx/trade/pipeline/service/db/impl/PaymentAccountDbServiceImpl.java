package com.mtx.trade.pipeline.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.pipeline.entity.PaymentAccountDO;
import com.mtx.trade.pipeline.mapper.PaymentAccountMapper;
import com.mtx.trade.pipeline.service.db.PaymentAccountDbService;
import org.springframework.stereotype.Service;

@Service
public class PaymentAccountDbServiceImpl extends ServiceImpl<PaymentAccountMapper, PaymentAccountDO>
        implements PaymentAccountDbService {
}
