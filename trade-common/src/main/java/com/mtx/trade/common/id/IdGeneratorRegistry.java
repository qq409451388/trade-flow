package com.mtx.trade.common.id;

import java.util.Locale;

/**
 * ID 生成器注册中心。
 *
 * <p>提供全局生成器和领域生成器的统一获取入口：
 * <ul>
 *   <li>{@link #global()} 返回委托底层雪花核心的全局生成器；</li>
 *   <li>{@link #forDomain(String)} 优先返回显式注册的领域生成器，否则动态创建默认领域生成器；</li>
 *   <li>同一个领域名称始终返回同一个实例。</li>
 * </ul></p>
 */
public interface IdGeneratorRegistry {

    /**
     * 获取全局 ID 生成器。
     *
     * @return 全局生成器
     */
    GlobalIdGenerator global();

    /**
     * 获取指定领域的 ID 生成器。
     *
     * <p>如果该领域已被显式注册（Spring Bean），返回显式注册的实例；
     * 否则动态创建一个委托全局雪花核心的默认领域生成器。
     * 同一个领域名称始终返回同一个实例。</p>
     *
     * @param domain 领域名称（非空，自动去首尾空格并转小写）
     * @return 领域 ID 生成器
     */
    DomainIdGenerator forDomain(String domain);

    /**
     * 标准化领域名称：非空校验、去首尾空格、转小写。
     *
     * @param domain 原始领域名称
     * @return 标准化后的领域名称
     * @throws IllegalArgumentException 如果 domain 为 null 或空白
     */
    static String normalize(String domain) {
        if (domain == null) {
            throw new IllegalArgumentException("domain must not be null");
        }
        String trimmed = domain.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("domain must not be blank");
        }
        return trimmed.toLowerCase(Locale.ROOT);
    }
}
