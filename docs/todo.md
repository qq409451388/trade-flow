# Storage 与分表架构改造 TODO

> 本文件用于下一次会话恢复完整上下文。开始改造前应先阅读本文件和根目录 `AGENTS.md`，再检查工作区未提交改动；不要覆盖用户已有修改。

## 一、已确认的业务前提

### 初期形态

- 初期不增加独立的 storage 服务，只保留两个 Spring Boot 应用：
  - `trade-ingress`：接收数据并执行 `put storage`。
  - `trade-pipeline`：消费后续任务并执行 `get storage`。
- 两个应用初期复用同一台 MySQL Server。
- 当前配置中的逻辑库：
  - ingress 使用 `trade_flow`。
  - pipeline 使用 `trade_pipeline`。
- 推荐继续保留逻辑库边界，不因为在同一台 MySQL Server 就把所有表混进同一个 schema：
  - storage 表暂时由 ingress 所在的 `trade_flow`（或后续明确命名的 `trade_storage` schema）承载。
  - pipeline 业务表继续放在 `trade_pipeline`。
  - pipeline 读取 storage 时使用独立的、只读优先的 storage 数据源。
- ingress 负责写 storage，pipeline 负责读 storage。业务代码不应直接依赖 Storage Mapper/DO 完成读写。
- `trade_storage`、`trade_storage_blob` 使用相同 `id + payload_sha256`，按完整 SHA-256 无符号值模100路由到 `_00 ~ _99`。
- pipeline 未来也会分表，但规则不同：pipeline 业务表计划按业务时间分表（按日还是按月、时间字段、保留周期尚未最终确定）。

### 后期演进目标

- storage MySQL 可能迁移到独立机器或拆成多个 MySQL 实例。
- ingress、pipeline 可能进一步服务化部署。
- 后期可以增加独立 `trade-storage-service`，ingress/pipeline 通过远程客户端访问。
- 从本地数据库实现切换到远程 storage 服务时，ingress/pipeline 的核心业务代码不应修改，只替换接口实现和部署配置。
- storage 的 `unsigned(payload_sha256) % 100` 应视为稳定的虚拟分片编号。未来拆 MySQL 时迁移/重新分配 00~99 逻辑桶，不要改成 `SHA % 数据库实例数`，否则扩容会导致历史数据大面积重新路由。

## 二、当前代码状态（2026-07-21）

2026-07-21 已完成第一批单机改造，当前实现以 `docs/storage-design.md` 为准：

- 新增轻量 `trade-storage-api`，提供稳定读写端口和 DTO。
- 新增 `trade-storage-local`，承载 MyBatis-Plus、Hikari、ShardingSphere、DO/Mapper/DB Service 和本地 adapter。
- `trade-common` 已移除 Storage 持久化代码及 ShardingSphere/Hikari 传递依赖。
- ingress/pipeline 均使用独立命名的 Storage 数据源；Storage Mapper 固定绑定 `storageSqlSessionFactory`。
- 本阶段 Storage schema 继续使用 `trade_flow`，pipeline 通过同机只读连接读取。

以下内容为改造前状态记录：

当前 storage 相关代码位于 `trade-common`：

- `com.mtx.trade.common.storage.entity.StorageDO`
- `com.mtx.trade.common.storage.entity.StorageBlobDO`
- storage Mapper、DB Service、`StorageService` 及实现
- `StorageAutoConfiguration`
- `StorageShardingAutoConfiguration`

当前实现已经具备：

- 两张 storage 逻辑表按 `id % 100` 分为 100 张物理表。
- 两张表配置为 binding tables。
- common 自动扫描 storage Mapper 并注册 storage Service。
- `StorageServiceImpl.saveRawData` 已加事务。
- ingress 使用标准 `spring.datasource` 配置。
- ShardingSphere 的 `sql-show` 当前被用户改成了 `true`。

当前实现存在的架构问题：

1. `trade-common` 直接传递 ShardingSphere 和 Hikari，导致只想使用 common 基础能力的模块也被迫加载重依赖。
2. `StorageShardingAutoConfiguration` 使用：

   ```java
   @ConditionalOnClass(ShardingSphereDataSourceFactory.class)
   @ConditionalOnProperty(
       prefix = "trade.storage.sharding",
       name = "enabled",
       matchIfMissing = true
   )
   ```

   未来 pipeline 为自己的时间分表引入 ShardingSphere 后，也会满足该条件，可能错误激活 storage 数据源配置。因此不能用“classpath 是否有 ShardingSphere”判断应用是否需要 storage。
3. common 当前直接创建名为 `dataSource` 的全局主数据源。这在 pipeline 同时需要 pipeline 主数据源和 storage 只读数据源时无法合理扩展。
4. `StorageAutoConfiguration` 只判断 MyBatis-Plus 是否存在，会在不需要 storage 的 MyBatis 应用中注册 Storage Mapper/Service。
5. `StorageService` 当前对业务返回 `StorageDO`，导致持久化实体泄漏到业务边界；将来改为远程服务时调用方会与数据库结构强耦合。
6. MyBatis Mapper 目前没有按命名数据源/`SqlSessionFactory` 明确隔离，多数据源后可能发生 Mapper 绑定错误。
7. `sql-show` 被硬编码，应改成可配置项。开发环境可以开启实际路由 SQL，生产环境默认关闭。

注意：当前工作区可能存在用户未提交修改。最近检查时至少包含：

- `trade-common/.../StorageShardingAutoConfiguration.java`：用户把 `sql-show` 改为 `true`。
- `trade-ingress/.../OrderController.java`：存在用户修改，不属于本改造时不要覆盖。
- `trade-ingress/src/main/resources/application.yml`：存在用户修改，编辑前先查看 diff。

## 三、目标架构

### 1. 稳定业务端口

ingress/pipeline 只依赖稳定接口，不直接调用 Storage Mapper/DB Service：

```java
public interface StorageWriter {
    StorageRef putIfAbsent(StorageWriteCommand command);
}

public interface StorageReader {
    StorageMetadata getMetadata(StorageKey key);
    byte[] getContent(StorageKey key);
}
```

建议稳定 DTO：

```java
public record StorageRef(
        Long storageId,
        byte[] sha256,
        Integer contentLength) {
}
```

- ingress 只注入 `StorageWriter`。
- pipeline 只注入 `StorageReader`。
- `StorageDO`、`StorageBlobDO`、Mapper、表名和分片后缀都是本地持久化实现细节，不作为跨模块业务 API 返回值。
- 如果短期确实有代码需要引用 StorageDO，应明确这是 persistence 层依赖，不能让业务流程以 DO 作为稳定契约。

### 2. 推荐模块边界

建议逐步拆成：

```text
trade-common
  DTO、异常、ID 等轻量公共能力

trade-storage-api
  StorageWriter / StorageReader
  StorageWriteCommand / StorageRef / StorageMetadata
  不依赖 MyBatis、Hikari、ShardingSphere

trade-sharding-spring-boot-starter
  通用 ShardingSphere 数据源装配能力
  不包含任何具体业务表规则

trade-storage-local-starter
  StorageDO / StorageBlobDO
  Mapper / DbService
  本地 StorageWriter / StorageReader 实现
  storage 的 SHA-256 % 100 分表规则

trade-storage-client（后期）
  StorageWriter / StorageReader 的 HTTP 或 RPC 实现

trade-storage-service（后期）
  独立 storage 服务，复用 local starter 中的持久化能力
```

如果当前不希望一次增加多个 Maven 模块，第一阶段可以先在现有模块中按上述 package/接口边界拆分，但代码依赖方向必须保持一致，后续再机械迁移为独立 artifact。

### 3. 分表规则采用可组合贡献者

ShardingSphere 只代表“具备分表引擎”，不能代表启用 storage 规则。通用层定义类似：

```java
public interface ShardingRuleContributor {
    void contribute(ShardingRuleConfiguration rule);
}
```

具体业务分别提供：

- `StorageShardingRuleContributor`：只定义 `trade_storage` / `trade_storage_blob` 的完整 SHA-256 模100规则。
- `PipelineTimeShardingRuleContributor`：位于 pipeline 业务模块，只定义 pipeline 表的时间分片规则。

规则是否加载由“是否引入对应 starter/adapter”决定，不以 ShardingSphere classpath 作为业务功能开关，也不要求无关模块配置 `trade.storage.enabled=false`。

同一个数据源内允许不同逻辑表使用不同算法；如果是不同逻辑库，则应创建独立的命名数据源和 MyBatis 上下文，不要强行合并成唯一全局主数据源。

## 四、初期推荐部署与数据源拓扑

初期仍然只有 ingress、pipeline 两个 Boot，二者直连同一台 MySQL Server，不增加网络 storage 服务：

```text
trade-ingress
  ingress/storage 写数据源
    -> MySQL Server / trade_flow
    -> storage 表按 SHA-256 % 100 分表

trade-pipeline
  pipeline 主数据源
    -> 同一 MySQL Server / trade_pipeline
    -> pipeline 表按业务时间分表

  storage 只读数据源
    -> 同一 MySQL Server / trade_flow
    -> storage 表按 SHA-256 % 100 分表
```

这样初期只是同机跨 schema 访问，后期将 storage 数据源 URL 改到其他 MySQL Server 即可，不需要修改 pipeline 业务代码。

数据源与 Mapper 绑定要求：

- ingress 的业务 Mapper 和 storage Mapper可以共用 ingress/storage ShardingSphereDataSource（前提是都位于 `trade_flow`）。
- pipeline Mapper绑定 `pipelineSqlSessionFactory`。
- pipeline 中的 storage Mapper绑定 `storageReadSqlSessionFactory`。
- 两套事务管理器必须命名清晰；pipeline 的 storage 查询使用只读事务。
- 不要继续由 common 抢占通用 bean 名 `dataSource` 并假设整个应用只有一个数据源。

需要在实施前确认：storage 最终继续放 `trade_flow`，还是现在就建立独立 `trade_storage` schema。为了长期所有权清晰，更推荐独立 `trade_storage` schema；如果短期迁移成本较高，可以先保留 `trade_flow`，但接口和命名数据源边界先建立。

## 五、后期服务化切换方式

本地模式：

```text
ingress -> StorageWriter -> LocalStorageAdapter -> storage MySQL
pipeline -> StorageReader -> LocalStorageAdapter -> storage MySQL
```

远程模式：

```text
ingress -> StorageWriter -> RemoteStorageClient -> storage-service -> MySQL/OSS
pipeline -> StorageReader -> RemoteStorageClient -> storage-service -> MySQL/OSS
```

可以通过明确配置选择实现，例如：

```yaml
trade:
  storage:
    mode: local   # 后期切换为 remote
```

本地和远程实现必须实现同一组 `StorageWriter` / `StorageReader` 接口。禁止业务层通过判断 mode 编写两套逻辑。

分布式阶段必须补齐：

- `put` 幂等：调用方提供稳定 requestId，或基于来源系统 + 业务事件 ID 建唯一约束。
- ingress 在 storage 写成功后再发布给 pipeline，推荐事务 Outbox，避免数据库写入和消息发送双写不一致。
- 消息只携带 `storageId`、`storageSha256`、内容长度等引用，不携带 DO 或物理表信息。
- pipeline 对“消息已到但 storage 暂时不可见”支持退避重试，兼容主从延迟和异步迁移。
- 大报文后期可以落 OSS，StorageReader 返回流或受控下载引用，业务方不感知 BLOB/OSS 存储类型。
- 数据迁移采用双写/CDC、回填、校验、切读步骤；业务模块不参与物理分片迁移。

## 六、建议实施顺序

### 阶段 A：先建立接口边界，不改变部署

- [x] 新增稳定的 `StorageWriter`、`StorageReader`、Command/DTO。
- [x] 将当前 `StorageServiceImpl` 改造为本地 adapter，实现上述端口。
- [x] ingress 写入流程只注入 `StorageWriter`；临时查询端点只注入 `StorageReader`。
- [x] pipeline 订单事件读取流程只注入 `StorageReader`。
- [x] 业务代码不再返回或传递 StorageDO。
- [x] 保持 ingress、pipeline 两个 Boot，仍直连同一台 MySQL Server。

### 阶段 B：拆分数据源装配与业务规则

- [x] 删除 `StorageShardingAutoConfiguration` 对全局主 `dataSource` 的抢占。
- [ ] 抽取通用 ShardingSphere 数据源装配层。
- [ ] 抽取 `ShardingRuleContributor` 扩展点。
- [ ] storage 注册 SHA-256 分表 contributor。
- [x] pipeline 注册订单年度及商品明细 Hash 分表规则（当前直接装配，通用 contributor 后续再抽取）。
- [x] pipeline 使用默认 pipeline `dataSource` 和命名 `storageDataSource` 两套数据源。
- [x] Storage Mapper 明确绑定 `storageSqlSessionFactory`；业务 Mapper 继续绑定默认 factory。
- [x] ShardingSphere/Hikari 不再由轻量 `trade-common` 无条件传递。
- [x] storage local adapter 仅在引入模块且配置 `trade.storage.local.enabled=true` 时启用。

### 阶段 C：确定 pipeline 时间分表契约

- [x] 确定订单逻辑表及时间/Hash 分片列，详见 `docs/pipeline-design.md`。
- [x] 订单表确定按年分表，商品明细在年度内按 `order_no` 模16。
- [x] 定义查询携带时间范围或父订单年份 Hint 的约束，避免全分片扫描。
- [ ] 设计历史表归档、保留周期和跨周期查询策略。
- [x] Pipeline 最终基础结构与2026/2027分表已整理到 `docs/sql` 的最终初始化脚本。

### 阶段 D：为服务化做准备

- [ ] 增加 local/remote adapter 切换机制。
- [x] 基于 `(sourceSystem, payloadSha256)` 增加 putIfAbsent 幂等和分片内唯一约束。
- [ ] 增加 ingress Outbox 及 pipeline 重试机制。
- [ ] 设计 storage-service API，但初期不启动第三个 Boot。
- [ ] 增加只读账号和最小权限配置。

## 七、验收标准

- ingress 写入 storage 时，相同 SHA-256 的 metadata/blob 路由到相同 `_00 ~ _99` 后缀，并在同一事务中提交。
- pipeline 能通过 `StorageReader` 按 `(storageId, storageSha256)` 读取 ingress 写入的数据，业务代码不知道物理表名。
- pipeline 的时间分表规则和 storage 的 SHA-256 分表规则相互独立。
- ingress 启动时不需要 pipeline 规则；pipeline 可以同时装配 pipeline 主数据源和 storage 只读数据源。
- 任一不需要 storage 的模块不会因为引入 common 或 ShardingSphere 而自动加载 storage Mapper、Service 或规则。
- `sql-show` 可通过配置控制；MyBatis 可关闭冗余 `StdOutImpl` 日志，只保留需要的 ShardingSphere 实际 SQL。
- 把 storage 数据库连接迁移到另一台 MySQL Server 时，只修改配置和部署，不修改 ingress/pipeline 业务逻辑。
- 将 local adapter 替换为 remote client 时，ingress/pipeline 仍只依赖原有 StorageWriter/StorageReader。

## 八、下次开始工作时的检查清单

1. 阅读根目录 `AGENTS.md` 和本文件。
2. 执行 `git status --short`、`git diff`，保护用户未提交改动。
3. 检查 ingress、pipeline 当时实际 datasource/schema 配置，确认 storage schema 的最终选择。
4. 检查是否已有父聚合 POM；当前各模块是独立 Maven 工程，新增 artifact 前需决定是否增加聚合构建。
5. 先完成阶段 A，再做数据源/分表基础设施调整，避免一次同时改变业务 API、依赖结构和数据库拓扑。
6. 每次变更后至少验证：
   - `trade-common`/新增 storage 模块测试。
   - ingress clean test 和启动上下文。
   - pipeline clean test 和启动上下文。
   - storage ID 精确路由测试。
   - pipeline 时间范围路由测试（规则确定后）。

## 九、可靠投递简化改造：Redis 实时通知 + Pipeline 未 ACK 补拉（2026-07-22）

> 状态：待实施。此前规划的 Pipeline Inbox、独立 ACK Outbox、高优先级 Probe Stream 和
> `OPEN -> HALF_OPEN -> 探针 ACK -> CLOSED` 方案已经废弃，不得继续按旧方案实现。
>
> 开始前先阅读 `docs/project-overview.md`、`docs/design/event-version-design.md` 和
> `docs/design/pipeline-design.md`。本节记录已确认的最终简化方向和实际问题上下文。

### 9.1 已确认的职责边界

可靠投递收敛为：

```text
Ingress MySQL event：事件事实源
Redis Stream：实时通知、削峰，可重复、可丢失
Pipeline 定时补拉：最终兜底
Pipeline 处理审计：判断业务是否已经成功以及失败次数
```

Ingress 只负责：

1. 接收第三方报文并验签。
2. 写入 Storage。
3. 写入 `trade_order_event / trade_payment_event`。
4. 在自身保护策略允许时发布带有限重试的 Redis Stream 通知。
5. 接受 Pipeline 幂等单条或批量 ACK。

Pipeline 负责：

1. 实时消费 Redis Stream。
2. 定时批量拉取 Ingress 中超过缓冲期的所有 `acked=0` 事件。
3. 复用同一订单/支付 Handler 完成业务处理。
4. 根据已有处理审计识别“业务已成功、只差 Ingress ACK”的事件。
5. 同步单条或批量 ACK Ingress。
6. 对业务失败执行有限自动重试、终态审计和企微告警。

Ingress 不再通过探针证明 Pipeline 已经恢复。Pipeline 恢复后，始终由自己的定时补拉任务接管未 ACK 事件。

### 9.2 问题上下文

#### HALF_OPEN 探针恢复阻塞

2026-07-22 使用 `REPLAY_RPS=100` 压测时，Pipeline 实时 Listener 曾停止消费并形成大量积压。现场检查：

- 订单熔断处于 `HALF_OPEN`，`probe_event_id=135849`。
- 探针事件仍为 `acked=0`，并且是当时最早未 ACK 订单事件。
- 订单未 ACK 约3.4万条。
- Redis 普通订单 Stream 仍有约19.6万条 lag。
- 探针通过 XADD 追加在普通 Stream 尾部，必须等前方积压消费完才能 ACK。
- 探针 `auto_redelivery_count=0`，旧的耗尽事件拉取逻辑也不会处理它。

结果形成：

```text
普通 Stream 积压
  -> 探针排在队尾
  -> HALF_OPEN 无法关闭
  -> Ingress 数据库积压恢复无法开始
```

因此删除真实业务探针和 HALF_OPEN 探针闭环，不再建设独立 Probe Stream。

#### Ingress 重启造成 ACK 失败与 PEL 放大

当 Pipeline 已经完成业务落库，而 Ingress 正在重启时，ACK 会出现 `Connection refused`。旧行为是：

```text
业务成功
  -> Ingress ACK失败
  -> 不执行Redis XACK
  -> 消息留在PEL
  -> PEL再次执行完整业务
```

该行为不会丢数据，但会把短暂 HTTP 故障放大为 PEL 积压、重复 Storage 读取、重复业务幂等检查和重复审计。

本机 HTTP/SOCKS 代理导致内部请求误入代理的问题已经通过内部 RestClient 显式 `Proxy.NO_PROXY` 解决；
Ingress 真正重启产生的连接拒绝则由“Redis XACK + 定时补拉未 ACK event”收敛，不再引入 ACK Outbox。

### 9.3 删除和替换的旧机制

- [ ] 删除 Pipeline readiness 失败驱动的复杂跨系统熔断恢复闭环。
- [ ] 删除 `OPEN -> HALF_OPEN -> 探针 ACK -> CLOSED` 真实业务探针逻辑。
- [ ] 不新增高优先级 Probe Stream。
- [ ] 不新增 Pipeline Inbox。
- [ ] 不新增独立 Ingress ACK Outbox。
- [ ] 删除“只有 `auto_redelivery_count >= max` 才允许 Pipeline 主动拉取”的限制。
- [ ] 将 `ExhaustedEventPullScheduler` 改造成“所有超时未 ACK 事件补拉”任务，并按新职责重命名。
- [ ] Ingress 不再因 Pipeline 业务失败负责再次驱动业务执行；业务重试归 Pipeline。
- [ ] Ingress ACK 失败不再把已经形成可靠处理审计的 Redis 消息留在 PEL。

### 9.4 Ingress Redis 发布保护

Ingress 的限流/熔断能力继续保留，但只保护 Ingress 自身和 Redis，不参与跨系统业务确认。

- [ ] Redis 发布连接失败或连续失败达到阈值时暂停 XADD；event 仍正常写 MySQL。
- [ ] 对 Redis 发布设置可配置速率限制，避免第三方突发流量直接压垮 Redis。
- [ ] 支持 Stream 长度或消费 lag 高水位暂停、低水位恢复，避免频繁开关。
- [ ] 消费组不存在、Redis 状态无法确认或 Stream 超过保护阈值时允许停止发布，由 Pipeline 定时补拉兜底。
- [ ] Redis 恢复后只恢复新的实时发布；历史未 ACK 数据由 Pipeline 拉取，不要求 Ingress 全量重推。
- [ ] 保留有限 Redis 发布重试；达到上限只停止通知，不把 event 标记为失败或丢弃。
- [ ] Redis Stream 保持受控长度；即使裁剪未消费通知，也必须能通过 MySQL 未 ACK 补拉恢复。

Ingress 保护状态不需要等待 Pipeline 探针 ACK。可使用如下简单状态或等价实现：

```text
PUBLISHING
  -> Redis故障/达到高水位
PAUSED
  -> Redis恢复且降到低水位
PUBLISHING
```

### 9.5 Pipeline 实时消费收尾规则

实时 Redis Consumer 继续执行现有订单/支付 Handler，但调整 ACK/XACK 边界：

| 本次结果 | Ingress ACK | Redis XACK |
| --- | --- | --- |
| 业务成功，ACK成功 | 成功 | 执行 |
| 业务成功，ACK失败 | 保持 Ingress `acked=0`，等待定时补拉 | 执行 |
| 幂等忽略，ACK成功 | 成功 | 执行 |
| 幂等忽略，ACK失败 | 保持 Ingress `acked=0`，等待定时补拉 | 执行 |
| 业务明确失败，失败审计成功 | 不 ACK，等待定时补拉 | 执行 |
| 审计落库失败 | 不 ACK | 不 XACK，保留 PEL |
| Pipeline 处理中崩溃 | 未完成 | 不 XACK，保留 PEL |
| Redis XACK失败 | 取决于前序结果 | 保留 PEL 接管 |

- [ ] 修改订单和支付 Consumer：业务结果审计可靠落库后，不因 Ingress ACK 失败保留 PEL。
- [ ] 明确 PEL 只恢复处理中断、审计落库失败和 Redis XACK 失败，不承担长期 HTTP ACK 或业务重试。
- [ ] Ingress ACK 与 Redis XACK 继续幂等；迟到和重复消息必须安全收敛。
- [ ] 保留 ListenerContainer 显式启动、Subscription 真实状态检查和 Watchdog 自动恢复。

### 9.6 Pipeline 所有未 ACK 事件补拉

补拉接口必须查询：

```sql
acked = 0
AND create_time <= 当前时间 - realtime_grace_period
AND id > after_event_id
ORDER BY id
LIMIT batch_size
```

不得继续要求 `auto_redelivery_count` 达到上限。建议默认给实时链路30秒缓冲期，避免定时拉取频繁抢占刚发布的消息。

- [ ] Ingress 将现有 exhausted 查询改为“超过缓冲期的所有未 ACK event”查询，并更新接口、DTO、命名和文档。
- [ ] 查询继续返回 `eventId + storageId + payloadSha256 + sourceSystem + eventKey + messageVersion`。
- [ ] 使用 `acked + create_time + id` 可用索引，按 eventId 游标批量查询，禁止 COUNT 和逐事件小查询。
- [ ] Pipeline 每批默认500条，单轮连续多批，保留最大批次数、最大运行时间、批次续租和有界并发。
- [ ] 当前 sweep 中单条失败只尝试一次并继续推进游标；下一轮重新从头查询 `acked=0`，不得永久跳过失败事件。
- [ ] 订单、支付使用独立调度入口，共享受控工作线程，避免打满5连接数据库池。
- [ ] 多 Pipeline 实例继续使用 MySQL 租约，确保每个内容类型同一时刻只有一个批量补拉 sweep。

### 9.7 批量识别“已成功，只差 ACK”

补拉到一批事件后，不能每条重新执行完整业务，也不能每条单独查询审计。

- [ ] 按本批 eventId 一次查询订单或支付处理审计，筛出已有 `APPLIED / IGNORED` 成功结果的 eventId。
- [ ] 已有成功审计的事件只补 Ingress ACK，不再读取 Storage、不再解析报文、不再执行 Handler。
- [ ] 没有成功审计的事件才进入订单/支付 Handler。
- [ ] 查询使用批量 `event_id IN (...)` 和合适索引，禁止 N+1。
- [ ] 同一 event 的多条失败审计不能覆盖后续成功事实；只要存在可靠成功审计即可进入仅 ACK 路径。
- [ ] 已进入人工终态并按策略 ACK 的事件不得再次执行。

### 9.8 批量 ACK Ingress

大量补拉场景下逐 event HTTP ACK 会产生大量小请求和 SQL。

- [ ] 保留实时链路单条 ACK，新增内部 `POST /event/batch-ack`。
- [ ] 请求按内容类型携带成功或终态 eventId，单批最多500条。
- [ ] Ingress 使用批量幂等 UPDATE，只更新 `acked=0` 记录。
- [ ] 返回本批请求数、首次 ACK 数、已 ACK 数和不存在数，便于 Pipeline 审计。
- [ ] Pipeline 只有在本批处理结果和成功审计可靠后才能提交批量 ACK。
- [ ] ACK HTTP 失败时不回滚已经成功的业务；下轮未 ACK 补拉继续收敛。
- [ ] 接口仅允许内部网络访问，不加入面向第三方的 Nginx 暴露路径。

### 9.9 实时消费与定时补拉并发控制

同一事件可能同时被实时 Consumer、定时补拉或另一 Pipeline 实例处理，必须避免并发破坏版本一致性。

- [ ] 补拉增加默认30秒实时缓冲期，优先让 Redis Consumer 完成。
- [ ] 保留内容类型级 sweep 租约，防止多实例同时批量拉取同一通道。
- [ ] 明确同一 `contentType + eventId` 的并发接管策略，不能只依赖异常后重试。
- [ ] 对同一 `sourceSystem + eventKey` 的不同版本保证串行或使用数据库条件更新，确保旧版本无法覆盖新版本。
- [ ] 审查订单主表、支付主表当前“查询版本后更新”的事务是否能抵抗实时与补拉并发；必要时增加行锁、乐观版本条件或按 eventKey 分区执行。
- [ ] Snowflake/eventId 不作为跨机器严格业务顺序，仍以第三方 messageVersion 判断新旧。

### 9.10 业务失败与终态策略

- [ ] 可恢复业务失败继续由下一轮未 ACK 补拉重试，不依赖 Ingress Redis 重发次数。
- [ ] 继续使用 Pipeline 审计统计主动补拉失败次数。
- [ ] 连续失败达到已配置阈值后，必须先可靠写失败审计，再 ACK Ingress 形成手工处理终态。
- [ ] 使用 `EnterpriseWechatRobotUtils` 按批汇总终态失败，禁止逐事件刷屏。
- [ ] 审计落库、失败次数查询或 ACK 任一步失败时，保持 `acked=0`，下一轮继续恢复。
- [ ] 人工重试需要显式重置或创建新的处理动作，不能依靠 Ingress 恢复 Redis 自动重发次数。

### 9.11 readiness 与监控定位

readiness 只用于监控和判断实时通知是否值得继续发送，不参与数据正确性。

- [ ] 保留 Pipeline DB、Storage、Redis、Consumer Group、Subscription active 检查。
- [ ] 当 Stream `lag>0` 时增加最近消费时间或游标进度检查，消除线程存在但消费不推进的假绿。
- [ ] Ingress 必须读取 `ResponseData.data.ready`；外层 success 只表示检查接口成功执行。
- [ ] 增加 Redis 发布是否暂停、限速命中、Stream lag、未 ACK 数量、补拉批次、仅 ACK 数量、业务重处理数量和最老未 ACK 年龄日志/指标。
- [ ] 核心稳定性日志继续使用英文和统一组件前缀。

### 9.12 验收场景

- [ ] Pipeline 停止时，Ingress 持续写 Storage 和 event；达到保护条件后停止 Redis 发布且不丢 event。
- [ ] Pipeline 恢复后，即使 Ingress 不重推历史 Redis，定时任务也能补拉所有超过缓冲期的 `acked=0` 事件。
- [ ] Redis Stream 被删除或发生裁剪后，未 ACK event 能通过定时补拉完整恢复。
- [ ] Ingress 重启1分钟时，Pipeline 已成功业务不会因 ACK 失败堆积到 PEL；Ingress 恢复后仅补 ACK。
- [ ] 20万 Redis lag 不阻塞 Pipeline 定时补拉，也不存在 HALF_OPEN 探针等待。
- [ ] 同一 event 被实时 Consumer 与补拉同时看见时，业务最多产生一次有效变更，旧版本不能覆盖新版本。
- [ ] 单批500条补拉只使用批量事件查询、批量成功审计查询和批量 ACK，不产生数据库或 HTTP N+1。
- [ ] 单条毒消息不阻塞同批后续事件，下一轮仍能重试，达到阈值后进入审计可靠的人工终态。
- [ ] 两个 Pipeline 实例并发时，每个内容类型只有一个 sweep 租约持有者。
- [ ] 订单和支付任何一个通道积压或失败不阻塞另一个通道。

### 9.13 目标链路

```text
第三方
  -> Ingress: Storage + event MySQL
  -> 受限流/暂停保护的 Redis Stream（实时通知）
       -> Pipeline 实时 Handler
       -> 处理审计
       -> Ingress ACK（失败不阻止 Redis XACK）

Pipeline 定时任务
  -> 批量查询 Ingress 所有超时 acked=0 event
  -> 批量查询 Pipeline 已成功 eventId
       -> 已成功：只批量 ACK
       -> 未成功：执行 Handler，成功或终态后批量 ACK
```

最终事实关系：

```text
Ingress event.acked=0：Pipeline 尚未完成接管确认，定时补拉必须继续看到
Pipeline 成功审计：业务已经完成，后续只需要补 ACK
Redis Stream/PEL：实时加速和处理中断恢复，不是最终数据事实源
```
