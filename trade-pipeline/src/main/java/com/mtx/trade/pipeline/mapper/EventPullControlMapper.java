package com.mtx.trade.pipeline.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mtx.trade.pipeline.entity.EventPullControlDO;
import org.apache.ibatis.annotations.Mapper;

/** Pipeline 主动拉取租约 Mapper。 */
@Mapper
public interface EventPullControlMapper extends BaseMapper<EventPullControlDO> {
}
