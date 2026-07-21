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
