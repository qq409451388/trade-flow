package com.mtx.trade.pipeline.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.pipeline.entity.EventPullControlDO;
import com.mtx.trade.pipeline.mapper.EventPullControlMapper;
import com.mtx.trade.pipeline.service.db.EventPullControlDbService;
import org.springframework.stereotype.Service;

/** Pipeline 主动拉取租约 DB Service 实现。 */
@Service
public class EventPullControlDbServiceImpl
        extends ServiceImpl<EventPullControlMapper, EventPullControlDO>
        implements EventPullControlDbService {
}
