# Java后端项目规范

## 项目通用规范
1. 接口业务逻辑中需要使用异常的场景，建议使用BusinessException，以返回标准数据格式

## 架构与模块边界
1. 业务模块访问 Storage 只能依赖 `trade-storage-api` 的 `StorageWriter`、`StorageReader` 和 DTO，禁止直接依赖 Storage DO、Mapper、DB Service 或物理分表名。
2. `trade-common` 保持轻量，不得无条件传递 MyBatis、ShardingSphere、数据库连接池等持久化重依赖；具体持久化能力放到对应 adapter/starter 模块。
3. 多数据源必须使用明确的 bean 名、`SqlSessionFactory` 和事务管理器绑定，禁止公共模块抢占通用 `dataSource` 或依赖 `@Primary` 猜测 Mapper 归属。
4. Storage 的 `unsigned(payload_sha256) % 100` 是稳定虚拟分片契约，必须使用完整 SHA-256 内容计算，禁止使用 Java `byte[]#hashCode()`；扩容时迁移或重新分配 `_00 ~ _99` 逻辑桶，禁止改成 `SHA % 数据库实例数`。
5. 当前阶段只采用进程内 Java adapter 直连单台 MySQL；未经明确设计评审，不新增独立 Storage 服务、RPC 或分布式中间件。
6. 第三方消息版本必须由来源 adapter 从报文中提取并转换为非负 `long`；event 幂等键必须包含来源、业务事件键和消息版本，消费端不得用 event 自增 ID 代替消息版本判断新旧。
7. Storage 主键由 storage 领域生成器显式生成一次，`trade_storage.id` 与 `trade_storage_blob.id`、`trade_storage.payload_sha256` 与 `trade_storage_blob.payload_sha256` 必须分别严格相等；两次写入任一失败必须抛异常并回滚，禁止依赖 MyBatis-Plus 自动为两张表分别生成 ID。
8. 独立领域雪花引擎必须沿用全局 epoch，并在所有机器、所有引擎之间分配唯一的 `(datacenterId, workerId)`；Snowflake ID 不得作为跨机器严格业务顺序。
9. 所有直接绑定 Storage 的数据库记录、消息和任务必须同时携带 `storageId + payloadSha256`，Storage 读取必须同时使用两者，禁止只按 `storageId` 触发100分片广播。

## MyBatis-Plus 使用规范
1. 后端新增数据库表对应的 DO/Mapper 时，应同步新增 `com.mtx.trade.service.db.XxxDbService` 和 `com.mtx.trade.service.db.impl.XxxDbServiceImpl`。
2. `XxxDbService` 继承 `IService<XxxDO>`，`XxxDbServiceImpl` 继承 `ServiceImpl<XxxMapper, XxxDO>` 并实现对应 DB Service。
3. 业务服务中优先注入 `XxxDbService` 使用 MyBatis-Plus 提供的 `getById`、`list`、`getOne`、`save`、`updateById`、`removeById`、`saveBatch` 等通用方法；复杂跨表编排仍保留在业务 Service 或 Repository 中。

## SQL规范
1. 每天的新变更sql文件按模块名-天命名放在docs/sql目录下， 形如：trade-pipeline-2026-07-20.sql

## 接口规范
### 通用规约
1. Controller 只能使用 `GET`、`POST`
    1. `GET` 只能用于查询，不允许触发新增、修改、删除、同步、上传、分析等副作用操作
    2. `POST` 用于所有带副作用的操作，包括新增、修改、删除、同步、上传、分析、导入、导出触发等
2. 接口名要让人只看路径就知道这个接口做什么，HTTP Method 只负责区分查询还是变更，不承担具体业务语义
3. 所有接口返回最外层必须是 `com.mtx.trade.common.dto.ResponseData`，返回类型形如 `ResponseData<XxxVO>`，`ResponseData<String>`
4. 根据业务情况，决定是否需要分页功能，分页对象: `com.telesales.training.dto.common.PageResult`，分页接口返回类型形如 `ResponseData<PageResult<XxxVO>>`
