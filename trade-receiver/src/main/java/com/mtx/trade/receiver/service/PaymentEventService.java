package com.mtx.trade.receiver.service;

import com.mtx.trade.receiver.entity.PaymentEventDO;

/**
 * 支付事件业务服务。
 */
public interface PaymentEventService {

    /**
     * 创建支付事件，返回保存后的对象。
     *
     * @param sourceSystem  来源系统
     * @param eventKey      事件唯一键
     * @param rawId         关联trade_storage.id
     * @param payloadSha256 原始请求体字节SHA-256
     * @return 保存后的支付事件
     */
    PaymentEventDO createEvent(int sourceSystem, String eventKey, Long rawId, byte[] payloadSha256);

    /**
     * 根据主键查询支付事件。
     *
     * @param id 主键
     * @return 支付事件
     */
    PaymentEventDO getById(Long id);

    /**
     * 标记支付事件执行成功。
     *
     * @param eventId     事件ID
     * @param executionId 执行流水ID
     */
    void markSuccess(Long eventId, Long executionId);

    /**
     * 标记支付事件执行失败。
     *
     * @param eventId     事件ID
     * @param executionId 执行流水ID
     */
    void markFailed(Long eventId, Long executionId);
}
