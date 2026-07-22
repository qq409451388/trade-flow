package com.mtx.trade.pipeline.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.common.id.IdGeneratorRegistry;
import com.mtx.trade.pipeline.config.PaymentShardingHint;
import com.mtx.trade.pipeline.config.PipelineShardingProperties;
import com.mtx.trade.pipeline.dto.PaymentAggregate;
import com.mtx.trade.pipeline.entity.PaymentAccountDO;
import com.mtx.trade.pipeline.entity.PaymentDO;
import com.mtx.trade.pipeline.service.db.PaymentAccountDbService;
import com.mtx.trade.pipeline.service.db.PaymentDbService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.hint.HintManager;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;

/** 支付主流水和结算账户的不可变幂等事务落库。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentPersistService {

    private final PaymentDbService paymentDbService;
    private final PaymentAccountDbService paymentAccountDbService;
    private final PaymentYearBoundaryResolver yearBoundaryResolver;
    private final PipelineShardingProperties shardingProperties;
    private final IdGeneratorRegistry idGeneratorRegistry;

    @Transactional(transactionManager = "pipelineTransactionManager", rollbackFor = Exception.class)
    public PaymentPersistResult persist(PaymentAggregate aggregate) {
        PaymentDO incoming = aggregate.payment();
        int routeYear = yearBoundaryResolver.resolve(aggregate);
        requireConfiguredYear(routeYear);
        try (HintManager ignored = PaymentShardingHint.useYear(routeYear)) {
            PaymentDO existing = paymentDbService.getOne(new LambdaQueryWrapper<PaymentDO>()
                    .eq(PaymentDO::getPaySsn, incoming.getPaySsn()), false);
            if (existing != null) {
                if (Arrays.equals(existing.getPayloadSha256(), incoming.getPayloadSha256())) {
                    log.debug("[Payment Persistence] ✅ Duplicate payment event was ignored. "
                                    + "paySsn={}, routeYear={}",
                            incoming.getPaySsn(), routeYear);
                    return PaymentPersistResult.IGNORED_DUPLICATE;
                }
                throw new BusinessException(ErrorCode.BUSINESS_ERROR,
                        "PAYMENT_PAYLOAD_CONFLICT: paySsn=" + incoming.getPaySsn());
            }

            long paymentId = nextId();
            incoming.setId(paymentId);
            try {
                if (!paymentDbService.save(incoming)) {
                    throw dataError("支付主表新增失败: paySsn=" + incoming.getPaySsn());
                }
            } catch (DuplicateKeyException e) {
                throw new ConcurrentPaymentInsertException(e);
            }

            aggregate.accounts().forEach(account -> {
                account.setId(nextId());
                account.setPaymentId(paymentId);
            });
            if (!aggregate.accounts().isEmpty()) {
                if (!paymentAccountDbService.saveBatch(aggregate.accounts())) {
                    throw dataError("支付结算账户批量新增失败: paySsn=" + incoming.getPaySsn());
                }
            }
            log.debug("[Payment Persistence] ✅ Payment persisted. paySsn={}, routeYear={}, accounts={}",
                    incoming.getPaySsn(), routeYear, aggregate.accounts().size());
            return PaymentPersistResult.APPLIED;
        }
    }

    private void requireConfiguredYear(int routeYear) {
        if (!shardingProperties.getYears().contains(routeYear)) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR,
                    "支付年度物理表未配置: " + routeYear);
        }
    }

    private long nextId() {
        return idGeneratorRegistry.global().nextId();
    }

    private static BusinessException dataError(String message) {
        return new BusinessException(ErrorCode.DATA_CREATE_ERROR, message);
    }
}
