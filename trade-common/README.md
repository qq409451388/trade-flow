# trade-common

交易公共组件，提供可复用的分布式 ID、统一响应、异常和基础枚举。

## 模块边界

`trade-common` 保持轻量，不包含 Storage DO、Mapper、数据源或分表实现，也不传递
ShardingSphere/Hikari 等持久化依赖。Storage 能力分别位于：

- `trade-storage-api`：稳定的 Storage 读写端口和 DTO。
- `trade-storage-local`：单机 MySQL adapter、持久化实体和分表规则。

详细设计见根目录 `docs/storage-design.md`。

## 核心设计

```
全局 SnowflakeIdEngine（单例）
        |
        +-- GlobalIdGenerator          ← 真全局生成器
        |
        +-- order 领域生成器            ← 委托同一引擎
        +-- payment 领域生成器          ← 委托同一引擎
        +-- 未配置领域生成器             ← 委托同一引擎

storage SnowflakeIdEngine（可选独立引擎）
        |
        +-- storage 领域生成器
```

领域默认共享全局 `SnowflakeIdEngine`；显式配置 `independent: true` 的领域使用独立引擎和 sequence。所有引擎必须分配互不重复的 `(datacenterId, workerId)`，才能保证 ID 在系统内全局唯一。

## 关键说明

| 要点 | 说明 |
|------|------|
| `GlobalIdGenerator` | 真全局生成器，直接委托雪花核心 |
| `DomainIdGenerator` | 领域级调用入口，默认委托同一雪花核心 |
| 领域生成器 | 默认共享全局引擎；可配置独立节点和 sequence |
| 跨领域唯一性 | 依赖所有引擎、所有机器的节点组合全局不重复 |
| 领域名称 | 不编码到 ID 中，数据库主键统一使用 `BIGINT` |
| ID 趋势 | 单引擎严格递增；跨机器、跨引擎仅按时间戳大致有序 |
| 禁止 | 不得根据 ID 差值统计业务数量 |
| datacenterId/workerId | 所有机器、所有独立领域引擎的组合必须全局唯一 |
| K8s 扩缩容 | 需进一步实现 workerId 自动租约分配 |
| 数据库主键 | 使用 `BIGINT`，不使用 `AUTO_INCREMENT` |

## 位结构

```
 1 位符号位（固定 0，保证正数）
41 位时间戳（毫秒，自 epoch 起，约 69.7 年）
 5 位数据中心 ID（0~31）
 5 位机器 ID（0~31）
12 位毫秒内序列号（0~4095，单节点每毫秒最多 4096 个 ID）
```

## 类关系

```
IdGenerator (接口)
  ├── GlobalIdGenerator (接口)
  │     └── DefaultGlobalIdGenerator ──┐
  └── DomainIdGenerator (接口)         │ 委托
        └── DefaultDomainIdGenerator ──┤
                                        ↓
                              SnowflakeIdEngine (核心，synchronized)
                                ├── TimeProvider (接口)
                                │     └── SystemTimeProvider
                                └── 异常体系
                                      ├── ClockBackwardException
                                      ├── InvalidDatacenterIdException
                                      ├── InvalidWorkerIdException
                                      ├── TimestampOverflowException
                                      └── DuplicateDomainRegistrationException

IdGeneratorRegistry (接口)
  └── DefaultIdGeneratorRegistry
        ├── global() → GlobalIdGenerator
        └── forDomain(domain) → DomainIdGenerator (ConcurrentHashMap 缓存)

GlobalIdProperties (@ConfigurationProperties)
  └── SnowflakeProperties (datacenterId, workerId, maxClockBackwardMs, epoch)

Spring Boot 自动配置:
  GlobalIdAutoConfiguration → TimeProvider, SnowflakeIdEngine, GlobalIdGenerator, IdGeneratorRegistry
  MyBatisPlusIdAutoConfiguration → SnowflakeIdentifierGenerator (条件: classpath 有 MyBatis-Plus)
```

## 配置

```yaml
global-id:
  enabled: true
  snowflake:
    datacenter-id: 1      # 0~31，所有实例必须唯一
    worker-id: 1           # 0~31，所有实例必须唯一
    max-clock-backward-ms: 5
    epoch: 1704067200000   # 2024-01-01 UTC，固定后不得修改
  domains:
    order:                  # 仅预注册，仍共享全局引擎
      independent: false
    storage:                # 独立领域引擎
      independent: true
      datacenter-id: 2      # 0~31，不得与其他引擎重复
      worker-id: 1          # 0~31，不得与其他机器重复
```

未出现在 `domains` 中的领域会按需创建并回退到全局引擎。独立领域继承全局的 `epoch` 和 `max-clock-backward-ms`，不允许单独修改 epoch，避免破坏 ID 时间位的一致含义。

多机部署建议固定 `datacenter-id` 表示用途、用 `worker-id` 表示机器。例如全局引擎使用 datacenter 1，storage 引擎使用 datacenter 2；机器 A/B 分别使用 worker 1/2。也可使用其他分配方式，但所有组合必须由部署系统统一管理。单个进程会检查自身配置冲突，无法发现另一台机器上的重复节点号。

Snowflake 不能保证两台机器按真实请求先后生成的 ID 严格递增：两台机器可能存在时钟偏差，同一毫秒内 ID 的大小还由节点位决定。若业务必须获得跨机器严格顺序，应使用数据库序列/号段服务或单点发号器，并接受协调开销；不要用 Snowflake ID 代替业务顺序字段。

## 使用方式

### 1. 注入全局生成器

```java
@RequiredArgsConstructor
public class ExampleService {

    private final GlobalIdGenerator globalIdGenerator;

    public long createId() {
        return globalIdGenerator.nextId();
    }
}
```

### 2. 通过注册中心获取领域生成器

```java
@RequiredArgsConstructor
public class OrderService {

    private final IdGeneratorRegistry idGeneratorRegistry;

    public long createOrderId() {
        return idGeneratorRegistry.forDomain("order").nextId();
    }
}
```

### 3. 覆盖指定领域的生成器（可选）

```java
@Bean
public DomainIdGenerator orderIdGenerator(SnowflakeIdEngine snowflakeIdEngine) {
    return new DefaultDomainIdGenerator("order", snowflakeIdEngine);
}
```

注册中心 `forDomain("order")` 优先返回显式注册的 Bean。同一领域注册不同实例会导致启动失败（不静默覆盖）。

### 4. MyBatis-Plus 集成

自动注册 `IdentifierGenerator` 适配器，实体默认使用：

```java
@TableId(type = IdType.ASSIGN_ID)
private Long id;
```

如需领域生成器，由业务层显式赋值：

```java
entity.setId(idGeneratorRegistry.forDomain("order").nextId());
```

## 接入业务实体步骤

1. 确认 `trade-common` 依赖已添加到目标项目 `pom.xml`
2. 配置 `global-id.snowflake.datacenter-id` 和 `worker-id`（确保全集群唯一）
3. 实体主键使用 `@TableId(type = IdType.ASSIGN_ID)` + `Long id` + 数据库 `BIGINT`
4. 需要领域级 ID 时注入 `IdGeneratorRegistry`

```xml
<dependency>
    <groupId>com.mtx.trade</groupId>
    <artifactId>trade-common</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## 测试覆盖

| # | 场景 | 测试类 |
|---|------|--------|
| 1 | 全局生成器连续 10 万个 ID 不重复 | ConcurrentIdGenerationTest |
| 2 | 多线程调用全局生成器不重复 | ConcurrentIdGenerationTest |
| 3 | 多个领域生成器并发生成不重复 | ConcurrentIdGenerationTest |
| 4 | 全局与领域生成器混合调用不重复 | ConcurrentIdGenerationTest |
| 5 | 同一领域名称返回同一生成器实例 | DefaultIdGeneratorRegistryTest |
| 6 | 不同领域返回不同门面实例 | DefaultIdGeneratorRegistryTest |
| 7 | 不同领域共享同一个底层雪花核心 | DefaultIdGeneratorRegistryTest |
| 8 | datacenterId 越界启动失败 | SnowflakeIdEngineTest + GlobalIdAutoConfigurationTest |
| 9 | workerId 越界启动失败 | SnowflakeIdEngineTest + GlobalIdAutoConfigurationTest |
| 10 | 小幅时钟回拨可以恢复 | SnowflakeIdEngineTest |
| 11 | 大幅时钟回拨抛出异常 | SnowflakeIdEngineTest |
| 12 | 单毫秒序列耗尽后进入下一毫秒 | SnowflakeIdEngineTest |
| 13 | 独立领域使用专属雪花节点，未配置领域回退全局节点 | GlobalIdAutoConfigurationTest |
| 14 | 独立领域缺少节点号或本进程节点号冲突时启动失败 | GlobalIdAutoConfigurationTest |

```
Tests run: 29, Failures: 0, Errors: 0, Skipped: 0
```
