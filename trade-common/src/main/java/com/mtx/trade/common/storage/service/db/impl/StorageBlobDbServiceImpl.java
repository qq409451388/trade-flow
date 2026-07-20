package com.mtx.trade.common.storage.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mtx.trade.common.storage.entity.StorageBlobDO;
import com.mtx.trade.common.storage.mapper.StorageBlobMapper;
import com.mtx.trade.common.storage.service.db.StorageBlobDbService;
import org.springframework.stereotype.Service;

/**
 * trade_storage_blob DB 层服务实现。
 */
@Service
public class StorageBlobDbServiceImpl extends ServiceImpl<StorageBlobMapper, StorageBlobDO> implements StorageBlobDbService {
}
