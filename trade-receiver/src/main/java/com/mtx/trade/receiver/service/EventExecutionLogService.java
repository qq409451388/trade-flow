package com.mtx.trade.receiver.service;

import com.mtx.trade.receiver.entity.EventExecutionLogDO;

import java.util.List;

/**
 * 事件执行流水业务服务。
 */
public interface EventExecutionLogService {

    /**
     * 创建一条执行流水，返回保存后的对象。
     *
     * @param eventType       事件类型：1订单；2支付
     * @param eventId         关联订单事件或支付事件ID
     * @param rawId           关联trade_storage.id
     * @param triggerType     触发方式：1首次消费；2自动重试；3人工重跑；4批量回放
     * @param executionStatus 执行结果：1成功；2失败；3忽略
     * @param message         执行结果说明
     * @param operatorName    操作人，自动执行时为空
     * @return 保存后的执行流水
     */
    EventExecutionLogDO log(int eventType, Long eventId, Long rawId, int triggerType,
                            int executionStatus, String message, String operatorName);

    /**
     * 根据主键查询执行流水。
     *
     * @param id 主键
     * @return 执行流水
     */
    EventExecutionLogDO getById(Long id);

    /**
     * 按 event_type + event_id 查询历史执行记录，按创建时间降序。
     *
     * @param eventType 事件类型
     * @param eventId   事件ID
     * @return 历史执行记录列表
     */
    List<EventExecutionLogDO> listByEvent(int eventType, Long eventId);
}
