package com.mtx.trade.ingress.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mtx.trade.ingress.entity.EventDeliveryControlDO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/** Event 投递熔断控制 Mapper。 */
@Mapper
public interface EventDeliveryControlMapper extends BaseMapper<EventDeliveryControlDO> {

    @Select("SELECT * FROM trade_event_delivery_control WHERE content_type = #{contentType} FOR UPDATE")
    EventDeliveryControlDO selectForUpdate(@Param("contentType") int contentType);
}
