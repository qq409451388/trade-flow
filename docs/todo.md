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

## 九、可靠投递后续改造：高优先级探针与 Ingress ACK Outbox（2026-07-22）

> 状态：待实施。开始前先阅读 `docs/project-overview.md`、`docs/design/event-version-design.md` 和
> `docs/design/pipeline-design.md`。本节记录的是实际压测和故障恢复过程中发现的问题，不能只按任务标题机械修改。

### 9.1 问题上下文一：HALF_OPEN 探针被普通 Stream 积压阻塞

压测期间使用 `REPLAY_RPS=100` 回放订单数据。Pipeline 实时 Stream Listener 曾因
`StreamMessageListenerContainer` 未显式启动而停止消费，修复并重启后 Redis 中已经形成大量普通消息积压。

2026-07-22 现场只读检查结果：

- 订单熔断状态为 `HALF_OPEN`，`probe_event_id=135849`。
- `trade_order_event.id=135849` 仍为 `acked=0`，并且是当时最早的未 ACK 订单事件。
- 订单未 ACK 共约 3.4 万条。
- Redis `stream:order-event` 消费组正在工作，但仍有约 19.6 万条 `lag`。
- 探针通过 `XADD` 追加到了普通订单 Stream 尾部，必须等待前面的普通积压全部消费后才能获得 ACK。
- 该探针 `auto_redelivery_count=0`，不满足耗尽事件主动拉取条件，因此
  `ExhaustedEventPullScheduler` 也不会提前处理它。

当前状态机会因此形成恢复阻塞：

```text
普通 Stream 有大量积压
  -> HALF_OPEN 探针排在积压尾部
  -> 探针长时间无法 ACK
  -> 熔断无法 CLOSED
  -> Ingress 数据库积压恢复不能开始
```

当前 `waitForProbeAck` 只安排下一次检查，没有明确探针截止时间；如果探针一直没有 ACK，通道可能长期停留在
`HALF_OPEN`。不能通过放宽检查或直接跳过 ACK 来掩盖该问题。

### 9.2 问题上下文二：Ingress ACK 失败导致完整业务重复执行

Pipeline 当前顺序为：

```text
业务落库 -> 处理审计 -> HTTP ACK Ingress -> Redis XACK
```

当 Pipeline 正在消费，而 Ingress 重启或短暂不可达时，HTTP ACK 会出现 `Connection refused`。此时：

- Pipeline 业务数据已经提交成功。
- 处理日志记录 Ingress ACK 失败。
- 当前 Redis 消息不执行 XACK，保留在 PEL。
- PEL 恢复任务随后重新执行完整业务 Handler，再依靠业务幂等避免重复写入。
- Ingress 仍保留 `acked=0`，恢复后还可能再次向 Stream 发布同一个 event。

该策略不会丢数据，但短暂故障会放大为 PEL、重复业务读取、重复审计和重复 Stream 消息。以每秒100条、Ingress
不可用1分钟估算，可能产生约6000条待收尾消息；当前 PEL 每批100条、每30秒扫描一次，单靠 PEL 收尾恢复较慢。

此外，本机开发环境曾配置 HTTP/SOCKS 代理，内部 ACK 请求被代理返回 `502/application/octet-stream`。目前内部
Ingress/Pipeline RestClient 已显式配置 `Proxy.NO_PROXY`；该修复只能解决代理误路由，不能解决 Ingress 真正重启时的
`Connection refused`，后者必须通过持久化 ACK Outbox 处理。

### 9.3 已确认的目标与不变量

- MySQL 继续作为事件和恢复任务事实源；Redis Stream 只承担通知、削峰和至少一次投递。
- Pipeline 业务成功或幂等忽略后，必须先形成持久化的 Ingress ACK 任务，才允许 Redis XACK。
- Ingress ACK 失败不得再依赖重新执行完整订单/支付业务来收尾。
- 业务失败不能创建成功 ACK 任务，仍按现有失败审计、自动补发和主动拉取策略处理。
- ACK 和 Redis XACK 都是幂等动作；任意进程崩溃点都允许安全重试。
- PEL 只承担业务处理中断、审计未可靠落库或 Redis XACK 失败恢复，不承担长期 HTTP ACK 重试。
- HALF_OPEN 必须验证 Redis 到 Pipeline 的实际处理能力，且探针不能排在普通积压之后。
- 订单、支付必须按通道隔离探针和线程，不能因一个通道积压阻塞另一个通道。
- 多实例领取 ACK 任务、发布探针和恢复积压必须使用 MySQL CAS/租约，禁止仅依赖进程内锁。

### 9.4 阶段一：实现 Pipeline Ingress ACK Outbox

- [ ] 在 Pipeline 最终基础结构和 `docs/sql/trade-pipeline-2026-07-22.sql` 中新增统一 ACK 任务表，暂定名
  `pipeline_ingress_ack_task`。
- [ ] 使用 `(content_type, event_id)` 唯一键，至少包含：`status`、`retry_count`、`next_retry_time`、
  `lease_owner`、`lease_until`、`last_error`、`process_log_id`、创建/更新时间。
- [ ] 同步新增 DO、Mapper、`IngressAckTaskDbService`、`IngressAckTaskDbServiceImpl`，遵循项目
  MyBatis-Plus DB Service 规范。
- [ ] 订单/支付业务事务成功或幂等忽略时，在同一个 `pipelineTransactionManager` 事务中 upsert ACK 任务。
- [ ] 业务提交后保留一次低延迟 ACK 快速尝试；成功则将任务标记完成，失败则保留任务等待后台重试。
- [ ] ACK 任务和处理审计可靠落库后，即使 HTTP ACK 失败也执行 Redis XACK；不得因此把完整业务留在 PEL 重跑。
- [ ] ACK 任务落库失败时不得 XACK；由 PEL 重放确保不会出现“业务成功但没有任何 ACK 恢复事实”的窗口。
- [ ] ACK 成功但任务状态更新失败时继续重试，依赖 Ingress ACK 幂等收敛。
- [ ] Redis XACK 失败时允许 PEL 重放；业务幂等和 ACK 任务唯一键必须保证无副作用。

建议 ACK 任务状态：

```text
PENDING -> PROCESSING -> SUCCEEDED
              |              ^
              +--失败/租约过期-+
```

`PROCESSING` 只能通过带租约的 CAS 领取。失败后回到可重试状态并更新 `next_retry_time`，不要设置自动放弃终态；
Ingress ACK 是必须最终完成的交接动作，达到告警阈值后仍要继续重试。

### 9.5 阶段二：实现独立 ACK 重试任务

- [ ] 所有 `@Scheduled` 入口放入 `trade-pipeline/.../task` 包，Service 只提供批量领取与执行方法。
- [ ] ACK 任务使用独立 Scheduler 和有界工作线程，订单、支付共享受控并发，避免打满 Pipeline 数据库连接池。
- [ ] 默认每批500条，按 `next_retry_time + id` 索引和游标查询，禁止逐 event 小查询扫描。
- [ ] 建议退避间隔：1秒、5秒、15秒、30秒、1分钟，之后固定每分钟；参数可配置。
- [ ] 每批续租；实例崩溃后其他实例可接管过期任务。
- [ ] ACK 成功后更新任务状态，并明确处理对应 process log 的 `ingress_ack_status` 最终语义。
- [ ] 连续失败达到阈值后使用 `EnterpriseWechatRobotUtils` 按批汇总告警，禁止逐事件刷屏；告警后仍继续重试。
- [ ] 增加积压数量、最老任务年龄、成功/失败速率和租约接管日志。

### 9.6 阶段三：独立高优先级 Probe Stream

推荐保留 Redis 端到端探测能力，不采用只验证 HTTP 的 Pipeline 专用处理接口。新增：

```text
stream:order-event-probe
stream:payment-event-probe
```

- [ ] Ingress 的 HALF_OPEN 只向对应 Probe Stream 发布一条真实未 ACK event，不再追加到普通 Stream 尾部。
- [ ] 探针消息继续携带完整 `eventId + contentType + storageId + payloadSha256 + eventKey + messageVersion`。
- [ ] Pipeline 为订单、支付 Probe Stream 分别配置独立 Consumer Group、ListenerContainer 和执行器。
- [ ] Probe Listener 复用现有订单/支付 Handler 和 ACK Outbox，不复制业务落库逻辑。
- [ ] 处理审计新增 `TRIGGER_CIRCUIT_PROBE = 4`，清晰区分普通 Stream、PEL、主动拉取和熔断探针。
- [ ] Probe Redis 消息仍手动 XACK；业务成功后先形成 ACK Outbox，再完成 Probe XACK。
- [ ] 普通 Stream 中迟到的同一事件仍按版本幂等忽略，并正常完成 ACK/XACK。

### 9.7 阶段四：修正 HALF_OPEN 状态机

- [ ] 在 `trade_event_delivery_control` 最终结构和当日 Ingress SQL 中增加 `probe_sent_time`、
  `probe_deadline`、`probe_attempt_count`（名称可在实现时统一）。
- [ ] `OPEN -> HALF_OPEN` 后发布探针并原子记录 event ID、发送时间、截止时间和状态版本。
- [ ] 探针发布失败立即回到 OPEN。
- [ ] 探针在截止时间内 ACK，且 `contentType + probeEventId + HALF_OPEN状态版本` 均匹配时，才能 CLOSED。
- [ ] 探针超时（建议默认30秒）必须回到 OPEN，下一轮健康检查重新探测，禁止无限等待。
- [ ] 迟到 ACK 仍按普通幂等 ACK 接受，但不得关闭已经进入下一轮的熔断状态。
- [ ] 探针成功关闭熔断后，再记录当时最大未 ACK event ID为恢复 cutoff，并按既有游标批量恢复普通积压。

### 9.8 阶段五：强化 readiness，消除“假绿”

当前已补充 `StreamMessageListenerContainer` 显式启动、订阅句柄检查和5秒 Watchdog。后续继续增加：

- [ ] readiness 检查业务库、Storage、Redis、普通 Consumer Group、普通 Subscription 和 Probe Subscription。
- [ ] 普通 Stream `lag=0` 时，Subscription active 可视为正常。
- [ ] `lag>0` 时必须验证最近消费时间或游标进度在配置窗口内推进，不能只看线程存在。
- [ ] Listener 每次收到记录时更新内存心跳；Watchdog 检测订阅退出后重新注册。
- [ ] Ingress 必须读取 `ResponseData.data.ready`，外层 `ResponseData.success` 只表示检查接口执行成功。

### 9.9 故障与并发验收场景

- [ ] 普通订单 Stream 存在20万条 lag 时，订单探针仍能在目标时间（建议5秒内）被 Pipeline 接管并 ACK。
- [ ] 支付积压不得影响订单探针，订单积压不得影响支付探针。
- [ ] Ingress 停止1分钟期间，Pipeline 业务继续落库，Redis PEL 不因 HTTP ACK 失败线性增长，ACK Outbox 正常积压。
- [ ] Ingress 恢复后 ACK Outbox 自动排空，订单/支付业务记录不重复、不回滚、不被旧版本覆盖。
- [ ] Pipeline 在“业务提交后、Outbox提交前”“Outbox提交后、XACK前”“ACK成功后、任务更新前”分别崩溃，重启后均可收敛。
- [ ] Redis Probe Stream 发布失败时熔断回 OPEN；探针超时不得长期停留 HALF_OPEN。
- [ ] 两个 Ingress/Pipeline 实例并发运行时，同一探针只有一个有效发布者，同一 ACK 任务同一时刻只有一个租约持有者。
- [ ] ACK 重试失败、租约过期、企微告警失败均有符合项目规范的英文结构化日志，并说明下一步恢复状态。

### 9.10 完成后的目标链路

```text
普通事件             -> 普通 Stream -> Pipeline 业务事务 + ACK Outbox -> Redis XACK
Ingress ACK失败       -> ACK Outbox 后台重试，不重跑完整业务
HALF_OPEN探针         -> 独立高优先级 Probe Stream -> 同一业务 Handler + ACK Outbox
普通 Stream巨大积压  -> 不影响探针完成和熔断关闭
PEL                   -> 只恢复处理中断、审计/Outbox未落库或Redis XACK失败
```
