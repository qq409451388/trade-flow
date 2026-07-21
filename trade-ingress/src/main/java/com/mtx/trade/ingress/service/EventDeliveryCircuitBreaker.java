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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis Stream 发布熔断状态机。
 *
 * <p>数据库是唯一事实源；内存只缓存最多数秒。CLOSED允许普通发布，OPEN完全停止普通发布，
 * HALF_OPEN只允许持有恢复租约的探测任务发布。</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventDeliveryCircuitBreaker {

    private static final int MAX_REASON_LENGTH = 1024;

    private final EventDeliveryControlDbService controlDbService;
    private final EventDeliveryCircuitProperties properties;
    private final Map<Integer, CachedState> cache = new ConcurrentHashMap<>();
    private final String recoveryOwner = "ingress-" + UUID.randomUUID();

    @PostConstruct
    public void initialize() {
        if (!properties.isEnabled()) {
            return;
        }
        ensureControl(ContentType.ORDER.getCode());
        ensureControl(ContentType.PAYMENT.getCode());
    }

    public boolean allowNormalPublish(int contentType) {
        return !properties.isEnabled()
                || state(contentType).status() == DeliveryCircuitStatus.CLOSED.getCode();
    }

    public String recoveryOwner() {
        return recoveryOwner;
    }

    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public void recordPublishFailure(int contentType, Throwable failure) {
        if (!properties.isEnabled()) {
            return;
        }
        EventDeliveryControlDO control = locked(contentType);
        LocalDateTime now = LocalDateTime.now();
        if (control.getCircuitStatus() == DeliveryCircuitStatus.HALF_OPEN.getCode()) {
            open(control, now, failure);
            save(control);
            return;
        }
        if (control.getCircuitStatus() == DeliveryCircuitStatus.OPEN.getCode()) {
            control.setLastFailureTime(now);
            control.setLastFailureReason(reason(failure));
            save(control);
            return;
        }

        LocalDateTime windowStart = control.getFailureWindowStart();
        if (windowStart == null || windowStart.plus(properties.getFailureWindow()).isBefore(now)) {
            control.setFailureWindowStart(now);
            control.setFailureCount(1);
        } else {
            control.setFailureCount(value(control.getFailureCount()) + 1);
        }
        control.setLastFailureTime(now);
        control.setLastFailureReason(reason(failure));
        if (control.getFailureCount() >= Math.max(1, properties.getFailureThreshold())) {
            open(control, now, failure);
            log.error("event delivery circuit opened, contentType={}, failures={}, windowStart={}",
                    contentType, control.getFailureCount(), control.getFailureWindowStart());
        }
        save(control);
    }

    public List<EventDeliveryControlDO> listDueForHealthCheck() {
        if (!properties.isEnabled()) {
            return List.of();
        }
        LocalDateTime now = LocalDateTime.now();
        return controlDbService.list(new LambdaQueryWrapper<EventDeliveryControlDO>()
                .eq(EventDeliveryControlDO::getCircuitStatus, DeliveryCircuitStatus.OPEN.getCode())
                .and(wrapper -> wrapper.isNull(EventDeliveryControlDO::getNextHealthCheckTime)
                        .or().le(EventDeliveryControlDO::getNextHealthCheckTime, now)));
    }

    public List<EventDeliveryControlDO> listDrainable() {
        if (!properties.isEnabled()) {
            return List.of();
        }
        return controlDbService.list(new LambdaQueryWrapper<EventDeliveryControlDO>()
                .eq(EventDeliveryControlDO::getCircuitStatus, DeliveryCircuitStatus.CLOSED.getCode())
                .gt(EventDeliveryControlDO::getRecoveryCutoffId, 0)
                .apply("recovery_cursor_id < recovery_cutoff_id"));
    }

    public boolean claim(int contentType, int requiredStatus) {
        LocalDateTime now = LocalDateTime.now();
        return controlDbService.update(new LambdaUpdateWrapper<EventDeliveryControlDO>()
                .eq(EventDeliveryControlDO::getContentType, contentType)
                .eq(EventDeliveryControlDO::getCircuitStatus, requiredStatus)
                .and(wrapper -> wrapper.isNull(EventDeliveryControlDO::getRecoveryLeaseUntil)
                        .or().lt(EventDeliveryControlDO::getRecoveryLeaseUntil, now)
                        .or().eq(EventDeliveryControlDO::getRecoveryOwner, recoveryOwner))
                .set(EventDeliveryControlDO::getRecoveryOwner, recoveryOwner)
                .set(EventDeliveryControlDO::getRecoveryLeaseUntil, now.plus(properties.getRecoveryLease())));
    }

    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public boolean recordHealthSuccess(int contentType) {
        EventDeliveryControlDO control = locked(contentType);
        if (control.getCircuitStatus() != DeliveryCircuitStatus.OPEN.getCode()
                || !recoveryOwner.equals(control.getRecoveryOwner())) {
            return false;
        }
        int successes = value(control.getHealthSuccessCount()) + 1;
        control.setHealthSuccessCount(successes);
        if (successes >= Math.max(1, properties.getHealthSuccessThreshold())) {
            control.setCircuitStatus(DeliveryCircuitStatus.HALF_OPEN.getCode());
            control.setRecoveryLeaseUntil(LocalDateTime.now().plus(properties.getRecoveryLease()));
            save(control);
            log.info("event delivery circuit entered HALF_OPEN, contentType={}", contentType);
            return true;
        }
        control.setNextHealthCheckTime(LocalDateTime.now().plus(properties.getHealthCheckDelay()));
        control.setRecoveryOwner(null);
        control.setRecoveryLeaseUntil(null);
        save(control);
        return false;
    }

    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public void recordHealthFailure(int contentType, Throwable failure) {
        EventDeliveryControlDO control = locked(contentType);
        if (control.getCircuitStatus() != DeliveryCircuitStatus.OPEN.getCode()) {
            return;
        }
        control.setHealthSuccessCount(0);
        control.setNextHealthCheckTime(LocalDateTime.now().plus(properties.getHealthCheckDelay()));
        control.setLastFailureTime(LocalDateTime.now());
        control.setLastFailureReason(reason(failure));
        control.setRecoveryOwner(null);
        control.setRecoveryLeaseUntil(null);
        save(control);
    }

    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public void closeAfterProbe(int contentType, long recoveryCutoffId) {
        EventDeliveryControlDO control = locked(contentType);
        if (control.getCircuitStatus() != DeliveryCircuitStatus.HALF_OPEN.getCode()
                || !recoveryOwner.equals(control.getRecoveryOwner())) {
            return;
        }
        control.setCircuitStatus(DeliveryCircuitStatus.CLOSED.getCode());
        control.setFailureWindowStart(null);
        control.setFailureCount(0);
        control.setOpenedTime(null);
        control.setNextHealthCheckTime(null);
        control.setHealthSuccessCount(0);
        control.setRecoveryOwner(null);
        control.setRecoveryLeaseUntil(null);
        control.setRecoveryCursorId(0L);
        control.setRecoveryCutoffId(Math.max(0L, recoveryCutoffId));
        save(control);
        log.info("event delivery circuit closed after probe, contentType={}, recoveryCutoffId={}",
                contentType, recoveryCutoffId);
    }

    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public void reopenAfterProbeFailure(int contentType, Throwable failure) {
        EventDeliveryControlDO control = locked(contentType);
        open(control, LocalDateTime.now(), failure);
        save(control);
        log.error("event delivery circuit reopened after probe failure, contentType={}", contentType, failure);
    }

    @Transactional(transactionManager = "ingressTransactionManager", rollbackFor = Exception.class)
    public void advanceRecoveryCursor(int contentType, long cursorId, boolean finished) {
        EventDeliveryControlDO control = locked(contentType);
        if (control.getCircuitStatus() != DeliveryCircuitStatus.CLOSED.getCode()
                || !recoveryOwner.equals(control.getRecoveryOwner())) {
            return;
        }
        if (finished) {
            control.setRecoveryCursorId(0L);
            control.setRecoveryCutoffId(0L);
        } else {
            control.setRecoveryCursorId(Math.max(value(control.getRecoveryCursorId()), cursorId));
        }
        control.setRecoveryOwner(null);
        control.setRecoveryLeaseUntil(null);
        save(control);
    }

    public EventDeliveryControlDO getControl(int contentType) {
        return controlDbService.getById(contentType);
    }

    private CachedState state(int contentType) {
        long now = System.nanoTime();
        CachedState cached = cache.get(contentType);
        if (cached != null && cached.expiresAtNanos() > now) {
            return cached;
        }
        EventDeliveryControlDO control = controlDbService.getById(contentType);
        if (control == null) {
            ensureControl(contentType);
            control = controlDbService.getById(contentType);
        }
        CachedState loaded = new CachedState(
                control == null ? DeliveryCircuitStatus.CLOSED.getCode() : control.getCircuitStatus(),
                now + Math.max(1L, properties.getStateCacheTtl().toNanos()));
        cache.put(contentType, loaded);
        return loaded;
    }

    private void ensureControl(int contentType) {
        if (controlDbService.getById(contentType) != null) {
            return;
        }
        EventDeliveryControlDO control = new EventDeliveryControlDO();
        control.setContentType(contentType);
        control.setCircuitStatus(DeliveryCircuitStatus.CLOSED.getCode());
        control.setFailureCount(0);
        control.setHealthSuccessCount(0);
        control.setRecoveryCursorId(0L);
        control.setRecoveryCutoffId(0L);
        control.setVersion(0);
        try {
            controlDbService.save(control);
        } catch (DuplicateKeyException ignored) {
            // 另一实例已完成初始化。
        }
        cache.remove(contentType);
    }

    private EventDeliveryControlDO locked(int contentType) {
        EventDeliveryControlDO control = controlDbService.getForUpdate(contentType);
        if (control == null) {
            ensureControl(contentType);
            control = controlDbService.getForUpdate(contentType);
        }
        if (control == null) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "事件投递熔断状态不存在");
        }
        return control;
    }

    private void open(EventDeliveryControlDO control, LocalDateTime now, Throwable failure) {
        control.setCircuitStatus(DeliveryCircuitStatus.OPEN.getCode());
        control.setOpenedTime(now);
        control.setNextHealthCheckTime(now.plus(properties.getHealthCheckDelay()));
        control.setHealthSuccessCount(0);
        control.setLastFailureTime(now);
        control.setLastFailureReason(reason(failure));
        control.setRecoveryOwner(null);
        control.setRecoveryLeaseUntil(null);
        control.setRecoveryCursorId(0L);
        control.setRecoveryCutoffId(0L);
    }

    private void save(EventDeliveryControlDO control) {
        control.setVersion(value(control.getVersion()) + 1);
        boolean updated = controlDbService.update(new LambdaUpdateWrapper<EventDeliveryControlDO>()
                .eq(EventDeliveryControlDO::getContentType, control.getContentType())
                .set(EventDeliveryControlDO::getCircuitStatus, control.getCircuitStatus())
                .set(EventDeliveryControlDO::getFailureWindowStart, control.getFailureWindowStart())
                .set(EventDeliveryControlDO::getFailureCount, control.getFailureCount())
                .set(EventDeliveryControlDO::getOpenedTime, control.getOpenedTime())
                .set(EventDeliveryControlDO::getNextHealthCheckTime, control.getNextHealthCheckTime())
                .set(EventDeliveryControlDO::getHealthSuccessCount, control.getHealthSuccessCount())
                .set(EventDeliveryControlDO::getLastFailureTime, control.getLastFailureTime())
                .set(EventDeliveryControlDO::getLastFailureReason, control.getLastFailureReason())
                .set(EventDeliveryControlDO::getRecoveryOwner, control.getRecoveryOwner())
                .set(EventDeliveryControlDO::getRecoveryLeaseUntil, control.getRecoveryLeaseUntil())
                .set(EventDeliveryControlDO::getRecoveryCursorId, control.getRecoveryCursorId())
                .set(EventDeliveryControlDO::getRecoveryCutoffId, control.getRecoveryCutoffId())
                .set(EventDeliveryControlDO::getVersion, control.getVersion()));
        if (!updated) {
            throw new BusinessException(ErrorCode.DATA_CREATE_ERROR, "事件投递熔断状态更新失败");
        }
        cache.remove(control.getContentType());
    }

    private static int value(Integer value) {
        return value == null ? 0 : value;
    }

    private static long value(Long value) {
        return value == null ? 0L : value;
    }

    private static String reason(Throwable failure) {
        String value = failure == null ? null : failure.getMessage();
        if (!StringUtils.hasText(value)) {
            value = failure == null ? "unknown" : failure.getClass().getName();
        }
        return value.length() <= MAX_REASON_LENGTH ? value : value.substring(0, MAX_REASON_LENGTH);
    }

    private record CachedState(int status, long expiresAtNanos) {
    }
}
