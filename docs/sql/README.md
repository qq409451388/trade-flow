# SQL 文件说明

## 全新环境初始化

1. 执行 `trade-databases.sql` 创建单机部署所需数据库。
2. 在 `trade_flow` 数据库执行 `trade-flow-base-schema.sql`。
3. 在 `trade_flow` 数据库执行 `trade-storage-shards.sql`。
4. 在 `trade_pipeline` 数据库执行 `trade-pipeline-base-schema.sql`。
5. 在 `trade_pipeline` 数据库执行 `trade-pipeline-year-shards.sql`。

`trade_analytics` 当前只有应用骨架，没有业务表。

### Trade Pipeline

在目标 `trade_pipeline` 数据库中依次执行：

1. `trade-pipeline-base-schema.sql`
2. `trade-pipeline-year-shards.sql`

第一份文件维护订单、支付基础模板表，以及订单/支付事件处理日志的最终结构。第二份文件只通过
`CREATE TABLE ... LIKE 基础表` 创建2026、2027年度物理表，其中订单商品明细每年创建 `_00`～`_15`
共16张表。

基础 `oms_*` 表只作为年度建表模板保留，业务代码仍使用逻辑表名，运行时由 ShardingSphere 路由到
带年份的物理表。新增年度必须先创建物理表，再修改 `trade.pipeline.sharding.years`。

### Trade Flow / Storage

`trade-flow-base-schema.sql` 包含 Ingress 事实事件、投递熔断、接入失败审计及 Storage 两张模板表。
`trade-storage-shards.sql` 再从模板生成 `_00`～`_99` 共100组元数据/BLOB物理表。

## 文件维护规则

项目尚未上线，`docs/sql` 只保留能够还原新环境的最终脚本。日常开发仍按 `AGENTS.md` 创建当日变更 SQL，
但在结构确认后必须同步合并到最终脚本；上线准备阶段删除已经被最终结构吸收的临时迁移文件。
