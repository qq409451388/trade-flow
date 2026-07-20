package com.mtx.trade.common.storage.config;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mtx.trade.common.storage.service.db.impl.StorageBlobDbServiceImpl;
import com.mtx.trade.common.storage.service.db.impl.StorageDbServiceImpl;
import com.mtx.trade.common.storage.service.impl.StorageServiceImpl;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Import;

/**
 * storage 持久化组件自动配置。
 *
 * <p>接入方引入 common 后无需扩大自身的 ComponentScan 或 MapperScan 范围。</p>
 */
@AutoConfiguration
@ConditionalOnClass(BaseMapper.class)
@MapperScan("com.mtx.trade.common.storage.mapper")
@Import({StorageDbServiceImpl.class, StorageBlobDbServiceImpl.class, StorageServiceImpl.class})
public class StorageAutoConfiguration {
}
