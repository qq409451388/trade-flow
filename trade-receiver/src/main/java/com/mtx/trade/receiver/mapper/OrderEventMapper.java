package com.mtx.trade.receiver.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mtx.trade.receiver.entity.OrderEventDO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface OrderEventMapper extends BaseMapper<OrderEventDO> {
}
