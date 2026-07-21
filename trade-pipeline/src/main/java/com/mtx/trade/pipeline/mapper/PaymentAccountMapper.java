package com.mtx.trade.pipeline.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mtx.trade.pipeline.entity.PaymentAccountDO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface PaymentAccountMapper extends BaseMapper<PaymentAccountDO> {
}
