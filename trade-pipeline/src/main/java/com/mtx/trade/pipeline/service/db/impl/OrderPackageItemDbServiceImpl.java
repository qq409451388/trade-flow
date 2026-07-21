package com.mtx.trade.pipeline.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.pipeline.entity.OrderPackageItemDO;
import com.mtx.trade.pipeline.mapper.OrderPackageItemMapper;
import com.mtx.trade.pipeline.service.db.OrderPackageItemDbService;
import org.springframework.stereotype.Service;

@Service
public class OrderPackageItemDbServiceImpl
        extends ServiceImpl<OrderPackageItemMapper, OrderPackageItemDO>
        implements OrderPackageItemDbService {
}
