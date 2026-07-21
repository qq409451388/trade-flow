package com.mtx.trade.ingress.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mtx.trade.ingress.entity.EventIngestFailureLogDO;
import org.apache.ibatis.annotations.Mapper;

/** Event 接入失败审计 Mapper。 */
@Mapper
public interface EventIngestFailureLogMapper extends BaseMapper<EventIngestFailureLogDO> {
}
