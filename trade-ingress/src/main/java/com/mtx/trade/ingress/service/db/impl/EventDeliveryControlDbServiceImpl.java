package com.mtx.trade.ingress.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.ingress.entity.EventDeliveryControlDO;
import com.mtx.trade.ingress.mapper.EventDeliveryControlMapper;
import com.mtx.trade.ingress.service.db.EventDeliveryControlDbService;
import org.springframework.stereotype.Service;

/** Event 投递熔断控制 DB Service 实现。 */
@Service
public class EventDeliveryControlDbServiceImpl
        extends ServiceImpl<EventDeliveryControlMapper, EventDeliveryControlDO>
        implements EventDeliveryControlDbService {

    @Override
    public EventDeliveryControlDO getForUpdate(int contentType) {
        return baseMapper.selectForUpdate(contentType);
    }
}
