# Storage 单机架构设计

> 更新日期：2026-07-21。本文记录当前已经落地的 Storage 设计。当前范围只包含单机 Java 进程与单台 MySQL Server，不部署独立 Storage 服务。

## 1. 当前部署边界

同一台机器上部署两个 Spring Boot 进程和一个 MySQL Server：

```text
trade-receiver
  ├─ receiver dataSource -> MySQL/trade_flow
  └─ storageDataSource   -> MySQL/trade_flow

trade-pipeline
  ├─ pipeline dataSource -> MySQL/trade_pipeline
  └─ storageDataSource   -> MySQL/trade_flow（只读配置）
```

当前不增加 `trade-storage-service`，也不引入 HTTP/RPC 调用。receiver 和 pipeline 都通过进程内的 `trade-storage-local` adapter 访问同一台 MySQL。Storage 暂时继续放在 `trade_flow`，避免本阶段增加 schema 迁移；应用已经使用独立配置前缀，未来迁移到 `trade_storage` schema 或另一台 MySQL 时只修改连接配置。

## 2. 模块与依赖方向

```text
trade-receiver ─┐
                ├─> trade-storage-api <─ trade-storage-local
trade-pipeline ─┘                         ├─ MyBatis-Plus
                                         ├─ HikariCP
                                         └─ ShardingSphere-JDBC

trade-common：DTO、异常、枚举、ID 等轻量公共能力
```

- `trade-storage-api` 只包含 `StorageWriter`、`StorageReader`、Command 和 DTO，零持久化框架依赖。
- `trade-storage-local` 包含 DO、Mapper、DB Service、本地 adapter、命名数据源及 Storage 分表规则。
- `trade-common` 不再传递 ShardingSphere/Hikari，也不再自动扫描 Storage Mapper。
- receiver 通过 `StorageWriter` 写入；pipeline 的后续消费代码只能通过 `StorageReader` 读取。
- `StorageDO`、Mapper、物理表名和分片后缀不得出现在业务模块接口中。

## 3. 数据与分片契约

- 逻辑表：`trade_storage`、`trade_storage_blob`。
- 两张表共享同一个 `id`。
- 虚拟分片编号固定为 `id % 100`，物理表后缀为 `_00` 到 `_99`。
- 两张逻辑表为 binding tables，同一 id 必须路由到相同后缀。
- `LocalStorageAdapter.put` 在 `storageTransactionManager` 事务中同时写元数据和 BLOB。
- 虚拟分片数量 100 是稳定契约。未来拆 MySQL 实例时只重新分配 00~99 逻辑桶，禁止改为 `id % 实例数`。

建表脚本当前见 `docs/sql/trade_receiver_storage分表创建.sql`。该脚本依赖预先存在的模板表 `trade_storage` 和 `trade_storage_blob`。

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

这些 bean 均不使用 `@Primary`，也不占用 `dataSource`、`sqlSessionFactory` 等应用默认名称。receiver/pipeline 自己的 Mapper 继续使用各自的主数据源。pipeline 的 Storage Hikari 配置为 `read-only: true`；生产环境还应使用 MySQL 只读账号进行强制约束。

`trade.storage.local.sql-show` 控制 ShardingSphere 路由 SQL。开发环境可以开启，生产环境默认关闭；MyBatis 使用 `NoLoggingImpl` 避免重复打印逻辑 SQL。

## 5. 稳定端口

写入：

```java
StorageRef put(StorageWriteCommand command);
```

读取：

```java
StorageMetadata getMetadata(Long storageId);
byte[] getContent(Long storageId);
```

`StorageRef` 只携带 `storageId`、SHA-256 和内容长度，可用于后续任务或消息。字节数组 DTO 做防御性复制，避免调用方修改 adapter 内部数据。

## 6. 当前暂不实施

- 不启动第三个 Storage Boot，不设计远程接口实现。
- 不引入注册中心、分布式事务、分布式锁或多 MySQL 实例。
- 不实施 pipeline 时间分表；需先确定逻辑表、分片时间字段、按日或按月及保留周期。
- 不实施 Outbox、幂等 requestId、CDC 迁移和 OSS 存储；这些属于服务化或可靠消息阶段。

## 7. 后续演进约束

后续远程化时新增实现相同端口的 client，receiver/pipeline 业务代码不得根据 local/remote 模式分叉。迁移 Storage 数据库时仅调整 `trade.storage.datasource` 和账号权限。若抽取通用分表 starter，再引入 `ShardingRuleContributor` 扩展点，Storage ID 分片与 pipeline 时间分片必须由不同 contributor 提供。
