package com.mtx.trade.ingress.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.ingress.common.enums.DeliveryCircuitStatus;
import com.mtx.trade.ingress.config.EventDeliveryCircuitProperties;
import com.mtx.trade.ingress.entity.EventDeliveryControlDO;
import com.mtx.trade.ingress.service.db.EventDeliveryControlDbService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/** 只保护 Redis 通知发布的 CLOSED/OPEN 状态；不参与 Pipeline 业务确认。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventDeliveryCircuitBreaker {
    private static final int MAX_REASON_LENGTH = 1024;
    private final EventDeliveryControlDbService dbService;
    private final EventDeliveryCircuitProperties properties;
    private final String owner = "ingress-" + UUID.randomUUID();

    @PostConstruct
    void initialize() {
        if (!properties.isEnabled()) return;
        ensure(ContentType.ORDER.getCode());
        ensure(ContentType.PAYMENT.getCode());
    }

    public boolean allowPublish(int contentType) {
        if (!properties.isEnabled()) return true;
        EventDeliveryControlDO state = dbService.getById(contentType);
        return state == null || state.getCircuitStatus() == DeliveryCircuitStatus.CLOSED.getCode();
    }

    public List<EventDeliveryControlDO> listDueForHealthCheck() {
        if (!properties.isEnabled()) return List.of();
        LocalDateTime now = LocalDateTime.now();
        return dbService.list(new LambdaQueryWrapper<EventDeliveryControlDO>()
                .eq(EventDeliveryControlDO::getCircuitStatus, DeliveryCircuitStatus.OPEN.getCode())
                .and(q -> q.isNull(EventDeliveryControlDO::getNextHealthCheckTime)
                        .or().le(EventDeliveryControlDO::getNextHealthCheckTime, now)));
    }

    public boolean claimHealthCheck(int contentType) {
        LocalDateTime now = LocalDateTime.now();
        return dbService.update(new LambdaUpdateWrapper<EventDeliveryControlDO>()
                .eq(EventDeliveryControlDO::getContentType, contentType)
                .eq(EventDeliveryControlDO::getCircuitStatus, DeliveryCircuitStatus.OPEN.getCode())
                .and(q -> q.isNull(EventDeliveryControlDO::getRecoveryLeaseUntil)
                        .or().lt(EventDeliveryControlDO::getRecoveryLeaseUntil, now)
                        .or().eq(EventDeliveryControlDO::getRecoveryOwner, owner))
                .set(EventDeliveryControlDO::getRecoveryOwner, owner)
                .set(EventDeliveryControlDO::getRecoveryLeaseUntil, now.plus(properties.getRecoveryLease())));
    }

    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public void recordPublishFailure(int contentType, Throwable failure) {
        if (!properties.isEnabled()) return;
        EventDeliveryControlDO state = locked(contentType);
        LocalDateTime now = LocalDateTime.now();
        if (state.getCircuitStatus() == DeliveryCircuitStatus.OPEN.getCode()) {
            state.setLastFailureTime(now);
            state.setLastFailureReason(reason(failure));
            save(state);
            return;
        }
        if (state.getFailureWindowStart() == null
                || state.getFailureWindowStart().plus(properties.getFailureWindow()).isBefore(now)) {
            state.setFailureWindowStart(now);
            state.setFailureCount(1);
        } else {
            state.setFailureCount(value(state.getFailureCount()) + 1);
        }
        state.setLastFailureTime(now);
        state.setLastFailureReason(reason(failure));
        if (state.getFailureCount() >= Math.max(1, properties.getFailureThreshold())) {
            state.setCircuitStatus(DeliveryCircuitStatus.OPEN.getCode());
            state.setOpenedTime(now);
            state.setNextHealthCheckTime(now.plus(properties.getHealthCheckDelay()));
            state.setHealthSuccessCount(0);
            log.warn("[Circuit Breaker] 🔄 Redis publishing paused. contentType={}, failures={}",
                    contentType, state.getFailureCount());
        }
        save(state);
    }

    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public boolean recordHealthSuccess(int contentType) {
        EventDeliveryControlDO state = locked(contentType);
        if (state.getCircuitStatus() != DeliveryCircuitStatus.OPEN.getCode()
                || !owner.equals(state.getRecoveryOwner())) return false;
        int successes = value(state.getHealthSuccessCount()) + 1;
        if (successes < Math.max(1, properties.getHealthSuccessThreshold())) {
            state.setHealthSuccessCount(successes);
            releaseForNextCheck(state);
            save(state);
            return false;
        }
        state.setCircuitStatus(DeliveryCircuitStatus.CLOSED.getCode());
        state.setFailureWindowStart(null);
        state.setFailureCount(0);
        state.setOpenedTime(null);
        state.setNextHealthCheckTime(null);
        state.setHealthSuccessCount(0);
        state.setRecoveryOwner(null);
        state.setRecoveryLeaseUntil(null);
        save(state);
        log.info("[Circuit Breaker] ✅ Redis recovered; new notifications resumed. contentType={}", contentType);
        return true;
    }

    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public void recordHealthFailure(int contentType, Throwable failure) {
        EventDeliveryControlDO state = locked(contentType);
        if (state.getCircuitStatus() != DeliveryCircuitStatus.OPEN.getCode()) return;
        state.setHealthSuccessCount(0);
        state.setLastFailureTime(LocalDateTime.now());
        state.setLastFailureReason(reason(failure));
        releaseForNextCheck(state);
        save(state);
    }

    private void releaseForNextCheck(EventDeliveryControlDO state) {
        state.setNextHealthCheckTime(LocalDateTime.now().plus(properties.getHealthCheckDelay()));
        state.setRecoveryOwner(null);
        state.setRecoveryLeaseUntil(null);
    }

    private void ensure(int contentType) {
        if (dbService.getById(contentType) != null) return;
        EventDeliveryControlDO state = new EventDeliveryControlDO();
        state.setContentType(contentType);
        state.setCircuitStatus(DeliveryCircuitStatus.CLOSED.getCode());
        state.setFailureCount(0);
        state.setHealthSuccessCount(0);
        state.setVersion(0);
        try { dbService.save(state); } catch (DuplicateKeyException ignored) { }
    }

    private EventDeliveryControlDO locked(int contentType) {
        EventDeliveryControlDO state = dbService.getForUpdate(contentType);
        if (state == null) { ensure(contentType); state = dbService.getForUpdate(contentType); }
        if (state == null) throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "Redis发布保护状态不存在");
        return state;
    }

    private void save(EventDeliveryControlDO state) {
        state.setVersion(value(state.getVersion()) + 1);
        if (!dbService.updateById(state)) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "Redis发布保护状态更新失败");
        }
    }

    private static int value(Integer value) { return value == null ? 0 : value; }
    private static String reason(Throwable failure) {
        String text = failure == null ? null : failure.getMessage();
        if (!StringUtils.hasText(text)) text = failure == null ? "unknown" : failure.getClass().getName();
        return text.length() <= MAX_REASON_LENGTH ? text : text.substring(0, MAX_REASON_LENGTH);
    }
}
