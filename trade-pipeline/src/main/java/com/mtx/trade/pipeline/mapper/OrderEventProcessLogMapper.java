package com.mtx.trade.pipeline.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mtx.trade.pipeline.entity.OrderEventProcessLogDO;
import org.apache.ibatis.annotations.Mapper;

/** Pipeline 订单事件处理日志 Mapper。 */
@Mapper
public interface OrderEventProcessLogMapper extends BaseMapper<OrderEventProcessLogDO> {
}
