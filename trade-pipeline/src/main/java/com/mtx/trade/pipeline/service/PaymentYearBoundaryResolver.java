package com.mtx.trade.pipeline.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mtx.trade.pipeline.config.FuiouPaymentProperties;
import com.mtx.trade.pipeline.config.PaymentShardingHint;
import com.mtx.trade.pipeline.config.PipelineShardingProperties;
import com.mtx.trade.pipeline.dto.PaymentAggregate;
import com.mtx.trade.pipeline.entity.PaymentDO;
import com.mtx.trade.pipeline.service.db.PaymentDbService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.apache.shardingsphere.infra.hint.HintManager;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.Set;

/** 仅在跨年风险窗口内，通过原支付流水校正退款路由年份。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentYearBoundaryResolver {

    private final PaymentDbService paymentDbService;
    private final FuiouPaymentProperties paymentProperties;
    private final PipelineShardingProperties shardingProperties;

    public int resolve(PaymentAggregate aggregate) {
        PaymentDO payment = aggregate.payment();
        int candidateYear = payment.getPayTime().getYear();
        if (payment.getPayState() != 2 || payment.getSourcePaySsn().isBlank()
                || !requiresSourceLookup(payment.getPayTime(), aggregate.receivedTime())) {
            return candidateYear;
        }

        Set<Integer> candidates = new LinkedHashSet<>();
        candidates.add(candidateYear);
        candidates.add(candidateYear - 1);
        candidates.add(candidateYear + 1);
        for (Integer year : candidates) {
            if (!shardingProperties.getYears().contains(year)) {
                continue;
            }
            PaymentDO source = findByPaySsnInYear(payment.getSourcePaySsn(), year);
            if (source != null) {
                if (source.getPayState() != null && source.getPayState() != 1) {
                    log.warn("sourcePaySsn points to non-payment row, sourcePaySsn={}, payState={}",
                            payment.getSourcePaySsn(), source.getPayState());
                }
                return year;
            }
        }
        log.warn("SOURCE_PAYMENT_NOT_FOUND: paySsn={}, sourcePaySsn={}, candidateYear={}",
                payment.getPaySsn(), payment.getSourcePaySsn(), candidateYear);
        return candidateYear;
    }

    private PaymentDO findByPaySsnInYear(String paySsn, int year) {
        try (HintManager ignored = PaymentShardingHint.useYear(year)) {
            return paymentDbService.getOne(new LambdaQueryWrapper<PaymentDO>()
                    .eq(PaymentDO::getPaySsn, paySsn), false);
        }
    }

    private boolean requiresSourceLookup(LocalDateTime payTime, LocalDateTime receivedTime) {
        if (receivedTime != null && Math.abs(payTime.getYear() - receivedTime.getYear()) == 1) {
            return true;
        }
        Duration window = paymentProperties.getYearBoundaryWindow();
        return nearYearBoundary(payTime, window)
                || receivedTime != null && nearYearBoundary(receivedTime, window);
    }

    private static boolean nearYearBoundary(LocalDateTime value, Duration window) {
        if (window == null || window.isNegative() || window.isZero()) {
            return false;
        }
        LocalDateTime yearStart = LocalDateTime.of(value.getYear(), 1, 1, 0, 0);
        return Duration.between(yearStart, value).compareTo(window) <= 0
                || Duration.between(value, yearStart.plusYears(1)).compareTo(window) <= 0;
    }
}
