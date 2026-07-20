package com.mtx.trade.common.storage.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mtx.trade.common.storage.entity.StorageDO;
import org.apache.ibatis.annotations.Mapper;

/**
 * trade_storage Mapper。
 */
@Mapper
public interface StorageMapper extends BaseMapper<StorageDO> {
}
