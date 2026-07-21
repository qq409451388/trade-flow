package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.dto.EventIngestResult;
import com.mtx.trade.ingress.entity.PaymentEventDO;

/**
 * 支付事件业务服务。
 */
public interface PaymentEventService {

    /**
     * 创建支付事件，返回保存后的对象。
     *
     * @param sourceSystem  来源系统
     * @param thirdEventKey 第三方事件唯一键
     * @param messageVersion 第三方消息版本
     * @param rawId         关联trade_storage.id
     * @param payloadSha256 原始请求体字节SHA-256
     * @return 保存后的支付事件
     */
    EventIngestResult<PaymentEventDO> createEvent(
            int sourceSystem, String thirdEventKey, long messageVersion, Long rawId, byte[] payloadSha256);

    /**
     * 根据主键查询支付事件。
     *
     * @param id 主键
     * @return 支付事件
     */
    PaymentEventDO getById(Long id);

}
