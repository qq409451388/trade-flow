package com.mtx.trade.receiver.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mtx.trade.receiver.entity.EventExecutionLogDO;
import org.apache.ibatis.annotations.Mapper;

/**
 * 事件执行流水 Mapper。
 */
@Mapper
public interface EventExecutionLogMapper extends BaseMapper<EventExecutionLogDO> {
}
