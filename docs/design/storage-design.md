# Storage 单机架构设计

> 更新日期：2026-07-21。本文记录当前已经落地的 Storage 设计。当前范围只包含单机 Java 进程与单台 MySQL Server，不部署独立 Storage 服务。

## 1. 当前部署边界

同一台机器上部署两个 Spring Boot 进程和一个 MySQL Server：

```text
trade-ingress
  ├─ ingress dataSource -> MySQL/trade_flow
  └─ storageDataSource   -> MySQL/trade_flow

trade-pipeline
  ├─ pipeline dataSource -> MySQL/trade_pipeline
  └─ storageDataSource   -> MySQL/trade_flow（只读配置）
```

当前不增加 `trade-storage-service`，也不引入 HTTP/RPC 调用。ingress 和 pipeline 都通过进程内的 `trade-storage-local` adapter 访问同一台 MySQL。Storage 暂时继续放在 `trade_flow`，避免本阶段增加 schema 迁移；应用已经使用独立配置前缀，未来迁移到 `trade_storage` schema 或另一台 MySQL 时只修改连接配置。

## 2. 模块与依赖方向

```text
trade-ingress ─┐
                ├─> trade-storage-api <─ trade-storage-local
trade-pipeline ─┘                         ├─ MyBatis-Plus
                                         ├─ HikariCP
                                         └─ ShardingSphere-JDBC

trade-common：DTO、异常、枚举、ID 等轻量公共能力
```

- `trade-storage-api` 只包含 `StorageWriter`、`StorageReader`、`StorageIdGenerator`、Command 和 DTO，零持久化框架依赖。
- `trade-storage-local` 包含 DO、Mapper、DB Service、本地 adapter、命名数据源及 Storage 分表规则。
- `trade-common` 不再传递 ShardingSphere/Hikari，也不再自动扫描 Storage Mapper。
- ingress 通过 `StorageWriter` 写入；pipeline 的后续消费代码只能通过 `StorageReader` 读取。
- `StorageDO`、Mapper、物理表名和分片后缀不得出现在业务模块接口中。

## 3. 数据与分片契约

- 逻辑表：`trade_storage`、`trade_storage_blob`。
- ingress 通过 common 的 `IdGeneratorRegistry.forDomain("storage")` 生成一次 storage 领域雪花 ID。
- `trade_storage.id = trade_storage_blob.id`：两张表必须复用这一次生成的同一个 ID，禁止分别生成。
- `trade_storage.payload_sha256 = trade_storage_blob.payload_sha256`：两张表必须携带同一个32字节路由键。
- 两个 DO 的主键均使用 `IdType.INPUT`，Storage 持久化不走 MyBatis-Plus 全局 ID 自动填充。
- 虚拟分片编号固定为 `unsigned(payload_sha256) % 100`，使用完整256位无符号整数取模，物理表后缀为 `_00` 到 `_99`。
- SHA-256 在统计上均匀分布；无状态路由不承诺各表记录数绝对相等，但长期会趋近平均。
- 两张逻辑表为 binding tables，相同 SHA-256 必须路由到相同后缀。禁止对 `byte[]` 使用 Java 对象 `hashCode()` 或 ShardingSphere 内置 `HASH_MOD`。
- ShardingSphere 5.5.2 的标准分片策略会在调用自定义算法前强制分片值实现 `Comparable`，因此无法直接使用 JDBC `byte[]`。本地 adapter 保持数据库 `BINARY(32)` 和 Java `byte[]` 不变，在每次读写前根据完整 SHA-256 计算整数桶号，并通过 `HintManager` 路由；禁止为了绕过该限制把字段扩大为 `CHAR(64)`。
- Hint 是 Storage adapter 的持久化实现细节。所有 Storage SQL 必须经 `StorageWriter` / `StorageReader` 执行，禁止业务代码绕过 adapter 直接调用 Mapper，否则缺少 Hint 时无法确定唯一物理表。
- `LocalStorageAdapter.putIfAbsent` 在 `storageTransactionManager` 事务中完成幂等检查并同时写元数据和 BLOB。
- ingress event 表使用绑定业务 `dataSource` 的 `ingressTransactionManager`（同时提供默认别名 `transactionManager`）；禁止误用 `storageTransactionManager`。两个事务管理器按领域显式隔离。
- ingress 在进入 Storage 数据库事务前，使用 `sourceSystem + 完整 SHA-256` 获取 Redis 锁；Redis key 前缀统一维护在 `RedisKeyConstants`。该锁只用于降低同一内容并发写入时的唯一键冲突，数据库唯一键仍是最终正确性保障。
- metadata 或 BLOB 任一 `save` 返回 `false` 时主动抛出 `StorageWriteException`，整个事务回滚；ingress 将异常转换为富友失败响应。
- `putIfAbsent` 先按 `(source_system, payload_sha256)` 精确查询单个分片；并发竞争最终由分片内同字段唯一键兜底，命中后返回已有 `(id, SHA-256)`。
- 虚拟分片数量 100 是稳定契约。未来拆 MySQL 实例时只重新分配 00~99 逻辑桶，禁止改为 `SHA % 数据库实例数`。

全新环境先执行 `../sql/trade-flow-base-schema.sql` 创建 Storage 模板表，再执行
`../sql/trade-storage-shards.sql` 创建100组物理分表。已有环境迁移必须单独评审并生成迁移脚本，禁止把
一次性迁移逻辑混入最终初始化 SQL。

## 4. 数据源与 MyBatis 隔离

本地 adapter 仅在以下配置存在时启用：

```yaml
trade:
  storage:
    local:
      enabled: true
      sql-show: false
    datasource:
      url: jdbc:mysql://127.0.0.1:3306/trade_flow
      username: storage_user
      password: change-me
      driver-class-name: com.mysql.cj.jdbc.Driver
```

Storage 固定注册以下命名 bean：

- `storageActualDataSource`：Hikari 物理连接池。
- `storageDataSource`：ShardingSphere 包装后的数据源。
- `storageSqlSessionFactory`：只绑定 `com.mtx.trade.storage.local.mapper`。
- `storageTransactionManager`：只管理 Storage 事务。

这些 bean 均不使用 `@Primary`，也不占用 `dataSource`、`sqlSessionFactory` 等应用默认名称。ingress/pipeline 自己的 Mapper 继续使用各自的主数据源。pipeline 的 Storage Hikari 配置为 `read-only: true`；生产环境还应使用 MySQL 只读账号进行强制约束。

`trade.storage.local.sql-show` 控制 ShardingSphere 路由 SQL。开发环境可以开启，生产环境默认关闭；MyBatis 使用 `NoLoggingImpl` 避免重复打印逻辑 SQL。

## 5. 稳定端口

写入：

```java
StorageRef putIfAbsent(StorageWriteCommand command);
```

读取：

```java
StorageMetadata getMetadata(StorageKey key);
byte[] getContent(StorageKey key);
```

`StorageKey` 和 `StorageRef` 都以 `(storageId, sha256)` 表示稳定定位引用：SHA-256 先确定唯一物理分片，ID 再使用分片内主键索引定位记录。禁止暴露或保留只按 `storageId` 读取的接口，否则会广播100张表。字节数组 DTO 做防御性复制，避免调用方修改 adapter 内部数据。

所有直接绑定 Storage 的表、消息和任务必须同时保存 ID 与 SHA-256。当前订单/支付 event 已保存 `raw_id + payload_sha256`，执行流水补充 `payload_sha256`，Redis Stream 发布 `storageId + storageSha256`。

storage 领域配置为独立 `SnowflakeIdEngine`，拥有自己的毫秒内 sequence；未配置为独立的领域仍回退到全局引擎。独立引擎沿用全局 epoch，但必须占用独立的 `(datacenterId, workerId)`，从而保持与全局 ID 及其他领域 ID 不重复。

当前 ingress 默认全局节点为 `(1,1)`、storage 节点为 `(2,1)`，都可通过环境变量覆盖。多机部署时建议固定全局/storage 的 datacenterId，并为每台机器分配唯一 workerId，例如机器 A 使用 `(1,1)/(2,1)`，机器 B 使用 `(1,2)/(2,2)`。应用只能校验单进程内冲突，跨机器唯一性必须由部署配置保证。

同一 Snowflake 引擎内 ID 严格递增；跨机器或跨独立引擎只能按毫秒时间戳大致有序，无法保证真实申请顺序严格递增。消费排序应使用明确的业务时间、消息版本及稳定的并列排序字段，不能把 storageId 当作跨节点业务顺序。

全局及独立领域引擎统一默认容忍最多1000ms时钟回拨。小幅 NTP 或宿主机校时发生时，对应引擎暂停发号并等待
系统时钟追平，以保证 ID 不重复且不倒序；超过1000ms仍抛出 `ClockBackwardException`，由接入失败审计记录，
避免掩盖机器时间配置异常。可通过 `SNOWFLAKE_MAX_CLOCK_BACKWARD_MS` 覆盖，但所有领域共享同一阈值。

## 6. 当前暂不实施

- 不启动第三个 Storage Boot，不设计远程接口实现。
- 不引入注册中心、分布式事务、分布式锁或多 MySQL 实例。
- Pipeline 订单分表已独立实现，具体规则见 `pipeline-design.md`；Storage 不得引用或复用该规则。
- 不实施 Outbox、幂等 requestId、CDC 迁移和 OSS 存储；这些属于服务化或可靠消息阶段。

## 6.1 与 Event 消息版本的关系

Storage 不负责判断第三方消息版本的新旧，但 `putIfAbsent` 会基于 `(source_system, payload_sha256)` 避免完全相同报文重复落盘。ingress 仍按来源、业务事件键和消息版本决定是否生成 event；不同内容即使属于同一业务版本，也会先形成不同 Storage 引用，再由 event 幂等约束决定是否接受。

## 7. 后续演进约束

后续远程化时新增实现相同端口的 client，ingress/pipeline 业务代码不得根据 local/remote 模式分叉。迁移 Storage 数据库时仅调整 `trade.storage.datasource` 和账号权限。若抽取通用分表 starter，再引入 `ShardingRuleContributor` 扩展点，Storage ID 分片与 pipeline 时间分片必须由不同 contributor 提供。
