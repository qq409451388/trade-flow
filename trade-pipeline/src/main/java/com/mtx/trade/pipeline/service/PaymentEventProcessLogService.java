package com.mtx.trade.pipeline.service;

import com.mtx.trade.pipeline.dto.PaymentEventMessage;

import java.time.LocalDateTime;

/** 支付事件每次实际处理的独立审计。 */
public interface PaymentEventProcessLogService {

    /** 从 Redis Stream 消费组读取到的新消息触发处理。 */
    int TRIGGER_STREAM = 1;
    /** 从 Redis Stream PEL 接管超时未确认消息后触发处理。 */
    int TRIGGER_PEL_RECLAIM = 2;
    /** Pipeline 主动从 Ingress 拉取补发耗尽事件后触发处理，不经过 Redis Stream。 */
    int TRIGGER_ACTIVE_PULL = 3;

    /** 事件产生了有效业务变更并成功落库。 */
    int STATUS_APPLIED = 1;
    /** 事件因幂等、旧版本等原因被安全忽略，业务数据无需变更。 */
    int STATUS_IGNORED = 2;
    /** 事件处理失败，Ingress 不应被 ACK。 */
    int STATUS_FAILED = 3;

    /** 投递确认动作尚未执行或结果尚未记录。 */
    int DELIVERY_PENDING = 0;
    /** 投递确认动作执行成功。 */
    int DELIVERY_SUCCEEDED = 1;
    /** 投递确认动作执行失败，后续恢复流程仍需处理。 */
    int DELIVERY_FAILED = 2;
    /** 当前触发方式不需要该投递确认动作，例如主动拉取无需 Redis XACK。 */
    int DELIVERY_NOT_APPLICABLE = 3;

    long recordSuccess(
            PaymentEventMessage event,
            String streamRecordId,
            int triggerType,
            PaymentPersistResult result,
            LocalDateTime startedTime,
            long startedNanos);

    long recordFailure(
            PaymentEventMessage event,
            String streamRecordId,
            int triggerType,
            String failureStage,
            Throwable failure,
            LocalDateTime startedTime,
            long startedNanos);

    void recordIngressAck(long processLogId, boolean succeeded);

    void recordRedisXack(long processLogId, boolean succeeded);
}
