package com.mtx.trade.pipeline.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mtx.trade.pipeline.entity.PaymentDO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface PaymentMapper extends BaseMapper<PaymentDO> {
}
