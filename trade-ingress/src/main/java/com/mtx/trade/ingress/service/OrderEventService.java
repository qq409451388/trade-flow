package com.mtx.trade.ingress.service;

import com.mtx.trade.ingress.dto.EventIngestResult;
import com.mtx.trade.ingress.entity.OrderEventDO;

/**
 * 订单事件业务服务。
 */
public interface OrderEventService {

    /**
     * 创建订单事件，返回保存后的对象。
     *
     * @param sourceSystem  来源系统
     * @param thirdEventKey 第三方事件唯一键
     * @param messageVersion 第三方消息版本
     * @param rawId         关联trade_storage.id
     * @param payloadSha256 原始请求体字节SHA-256
     * @return 保存后的订单事件
     */
    EventIngestResult<OrderEventDO> createEvent(
            int sourceSystem, String thirdEventKey, long messageVersion, Long rawId, byte[] payloadSha256);

    /**
     * 根据主键查询订单事件。
     *
     * @param id 主键
     * @return 订单事件
     */
    OrderEventDO getById(Long id);

}
