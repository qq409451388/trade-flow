package com.mtx.trade.pipeline.service;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.mtx.trade.pipeline.config.UnackedEventPullProperties;
import com.mtx.trade.pipeline.entity.EventPullControlDO;
import com.mtx.trade.pipeline.service.db.EventPullControlDbService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

/** 使用 Pipeline MySQL 原子更新实现跨实例主动拉取租约。 */
@Service
@RequiredArgsConstructor
public class EventPullLeaseService {

    private final EventPullControlDbService controlDbService;
    private final UnackedEventPullProperties properties;
    private final String owner = "pipeline-pull-" + UUID.randomUUID();

    public boolean tryAcquire(int contentType) {
        long leaseSeconds = leaseSeconds(properties.getLeaseDuration());
        return controlDbService.update(new LambdaUpdateWrapper<EventPullControlDO>()
                .eq(EventPullControlDO::getContentType, contentType)
                .and(wrapper -> wrapper.isNull(EventPullControlDO::getLeaseUntil)
                        .or().apply("lease_until < CURRENT_TIMESTAMP(3)"))
                .set(EventPullControlDO::getLeaseOwner, owner)
                .setSql("lease_until = DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL "
                        + leaseSeconds + " SECOND)"));
    }

    public void release(int contentType) {
        controlDbService.update(new LambdaUpdateWrapper<EventPullControlDO>()
                .eq(EventPullControlDO::getContentType, contentType)
                .eq(EventPullControlDO::getLeaseOwner, owner)
                .set(EventPullControlDO::getLeaseOwner, null)
                .set(EventPullControlDO::getLeaseUntil, null));
    }

    /** 当前持有者续租；失败表示租约已丢失，本轮必须停止继续取新批次。 */
    public boolean renew(int contentType) {
        long leaseSeconds = leaseSeconds(properties.getLeaseDuration());
        return controlDbService.update(new LambdaUpdateWrapper<EventPullControlDO>()
                .eq(EventPullControlDO::getContentType, contentType)
                .eq(EventPullControlDO::getLeaseOwner, owner)
                .apply("lease_until > CURRENT_TIMESTAMP(3)")
                .setSql("lease_until = DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL "
                        + leaseSeconds + " SECOND)"));
    }

    private static long leaseSeconds(Duration duration) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("主动拉取租约时长必须为正数");
        }
        return Math.max(1L, duration.toSeconds());
    }
}
