package com.mtx.trade.common.storage.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.common.storage.entity.StorageDO;
import com.mtx.trade.common.storage.mapper.StorageMapper;
import com.mtx.trade.common.storage.service.db.StorageDbService;
import org.springframework.stereotype.Service;

/**
 * trade_storage DB 层服务实现。
 */
@Service
public class StorageDbServiceImpl extends ServiceImpl<StorageMapper, StorageDO> implements StorageDbService {
}
