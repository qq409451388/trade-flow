package com.mtx.trade.ingress.constants;

/** Redis key 统一定义。 */
public final class RedisKeyConstants {

    public static final String ORDER_EVENT_STREAM = "stream:order-event";

    public static final String PAYMENT_EVENT_STREAM = "stream:payment-event";

    /** Storage 内容幂等写入锁，后缀为 sourceSystem:sha256Hex。 */
    public static final String STORAGE_PUT_IF_ABSENT_LOCK = "trade:storage:put-if-absent:";

    /** 多实例 Ingress 超时未 ACK 事件补发任务锁。 */
    public static final String EVENT_REDELIVERY_JOB_LOCK = "trade:ingress:event:redelivery:job";

    private RedisKeyConstants() {
    }
}
