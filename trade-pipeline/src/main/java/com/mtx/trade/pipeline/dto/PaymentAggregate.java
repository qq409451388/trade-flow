package com.mtx.trade.pipeline.dto;

import com.mtx.trade.pipeline.entity.PaymentAccountDO;
import com.mtx.trade.pipeline.entity.PaymentDO;

import java.time.LocalDateTime;
import java.util.List;

/** 单条支付事件解析后的完整落库对象。 */
public record PaymentAggregate(
        PaymentDO payment,
        List<PaymentAccountDO> accounts,
        LocalDateTime receivedTime) {
}
