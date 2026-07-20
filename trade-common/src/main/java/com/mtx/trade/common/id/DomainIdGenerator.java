package com.mtx.trade.common.id;

/**
 * 领域 ID 生成器。
 *
 * <p>领域级别的调用入口，默认委托全局雪花核心，因此跨领域仍然全局唯一。
 * 领域名称不编码到 ID 中，数据库主键统一使用 {@code BIGINT}。</p>
 */
public interface DomainIdGenerator extends IdGenerator {

    /**
     * 返回此生成器所属的领域名称（已标准化：去首尾空格、转小写）。
     *
     * @return 领域名称
     */
    String domain();
}
