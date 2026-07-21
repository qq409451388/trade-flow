package com.mtx.trade.ingress.service.db;

import com.baomidou.mybatisplus.extension.service.IService;
import com.mtx.trade.ingress.entity.EventDeliveryControlDO;

/** Event 投递熔断控制 DB Service。 */
public interface EventDeliveryControlDbService extends IService<EventDeliveryControlDO> {

    EventDeliveryControlDO getForUpdate(int contentType);
}
