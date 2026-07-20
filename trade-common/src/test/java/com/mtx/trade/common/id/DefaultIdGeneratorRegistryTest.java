package com.mtx.trade.common.id;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link DefaultIdGeneratorRegistry} 单元测试。
 *
 * <p>覆盖场景：5（同一领域返回同一实例）、6（不同领域返回不同实例）、
 * 7（不同领域共享同一雪花核心）。</p>
 */
class DefaultIdGeneratorRegistryTest {

    private SnowflakeIdEngine newEngine() {
        return new SnowflakeIdEngine(
                GlobalIdProperties.DEFAULT_EPOCH, 0, 0, 5, new SystemTimeProvider());
    }

    // ======================== 场景 5：同一领域返回同一实例 ========================

    @Test
    @DisplayName("场景 5: 同一领域名称返回同一生成器实例")
    void sameDomainReturnsSameInstance() {
        SnowflakeIdEngine engine = newEngine();
        DefaultIdGeneratorRegistry registry = new DefaultIdGeneratorRegistry(engine);

        DomainIdGenerator g1 = registry.forDomain("order");
        DomainIdGenerator g2 = registry.forDomain("order");
        DomainIdGenerator g3 = registry.forDomain("ORDER"); // 大小写不敏感
        DomainIdGenerator g4 = registry.forDomain("  Order  "); // 去空格

        assertSame(g1, g2, "同名领域应返回同一实例");
        assertSame(g1, g3, "大小写不敏感应返回同一实例");
        assertSame(g1, g4, "去空格后应返回同一实例");
    }

    // ======================== 场景 6：不同领域返回不同实例 ========================

    @Test
    @DisplayName("场景 6: 不同领域返回不同门面实例")
    void differentDomainsReturnDifferentInstances() {
        SnowflakeIdEngine engine = newEngine();
        DefaultIdGeneratorRegistry registry = new DefaultIdGeneratorRegistry(engine);

        DomainIdGenerator order = registry.forDomain("order");
        DomainIdGenerator payment = registry.forDomain("payment");
        DomainIdGenerator event = registry.forDomain("event");

        assertNotSame(order, payment, "order 与 payment 应是不同实例");
        assertNotSame(order, event, "order 与 event 应是不同实例");
        assertNotSame(payment, event, "payment 与 event 应是不同实例");

        assertEquals("order", order.domain());
        assertEquals("payment", payment.domain());
        assertEquals("event", event.domain());
    }

    // ======================== 场景 7：不同领域共享同一雪花核心 ========================

    @Test
    @DisplayName("场景 7: 不同领域共享同一个底层雪花核心（生成的 ID 跨领域不重复）")
    void differentDomainsShareSameEngine() {
        SnowflakeIdEngine engine = newEngine();
        DefaultIdGeneratorRegistry registry = new DefaultIdGeneratorRegistry(engine);

        DomainIdGenerator order = registry.forDomain("order");
        DomainIdGenerator payment = registry.forDomain("payment");
        GlobalIdGenerator global = registry.global();

        // 全局和多个领域各自生成 ID
        long gId = global.nextId();
        long oId = order.nextId();
        long pId = payment.nextId();

        // 如果共享同一引擎，三个 ID 应各不相同
        assertNotEquals(gId, oId, "全局与 order 的 ID 不应重复");
        assertNotEquals(gId, pId, "全局与 payment 的 ID 不应重复");
        assertNotEquals(oId, pId, "order 与 payment 的 ID 不应重复");

        // ID 应来自同一序列空间（趋势递增）
        assertTrue(oId > gId, "order ID 应大于全局 ID（同一序列空间递增）");
        assertTrue(pId > oId, "payment ID 应大于 order ID（同一序列空间递增）");
    }

    // ======================== 显式注册的领域生成器 ========================

    @Test
    @DisplayName("显式注册的领域生成器优先返回")
    void explicitGeneratorTakesPrecedence() {
        SnowflakeIdEngine engine = newEngine();
        DomainIdGenerator custom = new DefaultDomainIdGenerator("order", engine);
        Map<String, DomainIdGenerator> explicit = new HashMap<>();
        explicit.put("order", custom);

        DefaultIdGeneratorRegistry registry = new DefaultIdGeneratorRegistry(engine, explicit);

        DomainIdGenerator result = registry.forDomain("order");
        assertSame(custom, result, "应返回显式注册的生成器");
    }

    @Test
    @DisplayName("未配置的领域按需动态创建")
    void unconfiguredDomainCreatedOnDemand() {
        SnowflakeIdEngine engine = newEngine();
        DefaultIdGeneratorRegistry registry = new DefaultIdGeneratorRegistry(engine);

        assertEquals(0, registry.cachedDomainCount());

        DomainIdGenerator g = registry.forDomain("newDomain");
        assertNotNull(g);
        assertEquals("newdomain", g.domain());
        assertEquals(1, registry.cachedDomainCount());

        // 再次获取应返回同一实例
        assertSame(g, registry.forDomain("newDomain"));
    }

    // ======================== 参数校验 ========================

    @Test
    @DisplayName("null 领域名称抛出异常")
    void nullDomainThrows() {
        DefaultIdGeneratorRegistry registry = new DefaultIdGeneratorRegistry(newEngine());
        assertThrows(IllegalArgumentException.class, () -> registry.forDomain(null));
    }

    @Test
    @DisplayName("空白领域名称抛出异常")
    void blankDomainThrows() {
        DefaultIdGeneratorRegistry registry = new DefaultIdGeneratorRegistry(newEngine());
        assertThrows(IllegalArgumentException.class, () -> registry.forDomain("   "));
    }
}
