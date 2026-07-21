package com.mtx.trade.ingress.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mtx.trade.ingress.entity.OrderEventDO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface OrderEventMapper extends BaseMapper<OrderEventDO> {
}
