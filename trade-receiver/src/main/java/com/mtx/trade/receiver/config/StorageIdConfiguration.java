package com.mtx.trade.receiver.config;

import com.mtx.trade.common.id.DomainIdGenerator;
import com.mtx.trade.common.id.IdGeneratorRegistry;
import com.mtx.trade.storage.api.StorageIdGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** receiver 为 StorageWriter 提供 storage 领域雪花 ID。 */
@Configuration(proxyBeanMethods = false)
public class StorageIdConfiguration {

    @Bean
    public StorageIdGenerator storageIdGenerator(IdGeneratorRegistry idGeneratorRegistry) {
        DomainIdGenerator storageDomainIdGenerator = idGeneratorRegistry.forDomain("storage");
        return storageDomainIdGenerator::nextId;
    }
}
