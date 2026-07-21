package com.mtx.trade.storage.local.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Storage 本地 adapter 配置。 */
@ConfigurationProperties("trade.storage.local")
public class StorageLocalProperties {

    /** 是否输出 ShardingSphere 实际路由 SQL。生产环境应保持 false。 */
    private boolean sqlShow;

    public boolean isSqlShow() {
        return sqlShow;
    }

    public void setSqlShow(boolean sqlShow) {
        this.sqlShow = sqlShow;
    }
}
