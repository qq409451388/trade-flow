# Trade Flow 项目速览

本文供新接入的 Agent 快速理解当前架构。开始修改前仍需先阅读根目录 `AGENTS.md`，具体领域规则以
`docs/design` 下的设计文档和代码为准。

## 1. 当前部署范围

- 当前按单机 Java + 单台 MySQL 设计，Redis 用于事件 Stream、短期重试协调和分布式锁。
- 原始第三方报文先进入 Storage，再由 Ingress 写事实事件，Pipeline 消费后写结构化订单或支付表。
- 尚未引入独立 Storage 服务、RPC、Kafka 或跨库分布式事务。

## 2. 模块职责

- `trade-common`：通用异常、响应、枚举和雪花 ID 能力，保持轻量。
- `trade-storage-api`：Storage 对外端口，只暴露 `StorageWriter`、`StorageReader` 和 DTO。
- `trade-storage-local`：Storage 的本地 MySQL 实现，元数据和 BLOB 各100张分表。
- `trade-ingress`：富友验签、原文写 Storage、订单/支付 event 事实落库、Redis Stream 发布、补发和熔断恢复。
- `trade-pipeline`：消费订单/支付事件，读取 Storage 原文，解析并幂等写入结构化业务表，然后 ACK Ingress。
- `trade-cdc`、`trade-analytics`：预留后续数据同步和分析能力，当前不是核心处理链路。

## 3. 核心数据链路

```text
富友回调
  -> Ingress 验签
  -> Storage 保存原文
  -> trade_order_event / trade_payment_event
  -> Redis Stream
  -> Pipeline 读取 Storage
  -> 订单或支付结构化表
  -> ACK Ingress
  -> XACK Redis
```

业务处理失败时，Pipeline 先用独立事务记录处理日志，再 XACK Redis，但不 ACK Ingress。Ingress 按
“立即、1分钟、5分钟、10分钟、15分钟、30分钟”提供最多6次投递机会；耗尽后由 Pipeline 主动拉取处理，
不建设 Dead Letter Stream。Pipeline 默认每分钟按订单、支付通道分别自动拉取一批耗尽事件，并使用
MySQL 租约避免多实例重复执行。PEL 只用于进程中断及 ACK 收尾恢复，不承担无限业务重试。

## 4. Storage 关键约束

- 稳定路由规则为完整 SHA-256 的无符号值模100，数据库字段保持 `BINARY(32)`。
- 所有 Storage 引用必须同时携带 `storageId + payloadSha256`，禁止只按 ID 查询造成100表广播。
- `trade_storage.id = trade_storage_blob.id`，两表 SHA 也必须相等；任一写入失败整体回滚。
- Storage 使用领域独立雪花 ID，机器间不承诺严格时间顺序。

详细说明见 `docs/design/storage-design.md`。

## 5. Pipeline 分表

- 订单主表：`oms_order_YYYY`，按订单创建年份。
- 订单商品：`oms_order_item_YYYY_00`～`15`，年份加 `orderNo % 16`。
- 订单规格、套餐：跟随父订单年份。
- 支付主表、结算账户：`oms_payment_YYYY`、`oms_payment_account_YYYY`，通过同一个强制 `routeYear` Hint 路由。
- 当前物理年份为2026、2027；新增年份必须先建表，再更新 `trade.pipeline.sharding.years` 并重启 Pipeline。

支付以 `paySsn` 为不可变幂等键：相同 SHA 为幂等成功，不同 SHA 记录
`PAYMENT_PAYLOAD_CONFLICT`，禁止覆盖原流水和账户。

Pipeline 同时装配业务库和 Storage 两套事务管理器。`PipelineTransactionManagementConfiguration` 明确将
`pipelineTransactionManager` 配置为未具名 `@Transactional`（包括 MyBatis-Plus 内置批量方法）的默认事务管理器；
Storage adapter 的跨库操作仍必须显式绑定 `storageTransactionManager`。禁止通过 `@Primary` 猜测事务归属，
也不需要为每个业务 DB Service 重写 MyBatis-Plus 方法。

## 6. 可靠投递与熔断

Ingress 按订单、支付通道分别维护 MySQL 熔断状态：

```text
CLOSED --失败达到阈值--> OPEN --连续健康检查成功--> HALF_OPEN
HALF_OPEN --试投成功--> CLOSED
HALF_OPEN --任意失败--> OPEN
```

状态以 `trade_event_delivery_control` 为准，Java 只做短缓存；HALF_OPEN 和积压恢复都使用数据库租约，
避免多实例并发恢复。CLOSED 状态也会周期检查 Pipeline readiness，连续失败会停止 Stream 发布但继续持久化 event；
HALF_OPEN 必须等探测事件获得 Pipeline ACK 后才能关闭。Pipeline readiness 会检查业务库、Storage 数据源、Redis 和对应消费组。

## 7. SQL 入口

- Pipeline 基础结构：`docs/sql/trade-pipeline-base-schema.sql`
- Pipeline 2026/2027 分表：`docs/sql/trade-pipeline-year-shards.sql`
- Ingress与Storage基础结构：`docs/sql/trade-flow-base-schema.sql`
- Storage 100组分表：`docs/sql/trade-storage-shards.sql`
- 数据库创建：`docs/sql/trade-databases.sql`

Pipeline 新环境在目标 `trade_pipeline` 库中先执行基础结构，再执行年度分表脚本。基础 `oms_*` 表仅作为 `LIKE` 模板，
运行时 ShardingSphere 只路由带年份的物理表。

## 8. 审计日志边界

- `trade_event_ingest_failure_log`：Ingress 已成功写 Storage，但关键事件字段解析或 event 落库失败时记录；
  它不记录 Pipeline 执行结果。
- `pipeline_order_event_log`：订单事件每次由新 Stream、PEL 接管或主动拉取触发处理时记录结果、失败阶段和耗时。
- `pipeline_payment_event_log`：支付事件对应的处理审计，语义与订单日志一致。

旧的 `trade_event_execution_log` 已删除。Ingress 不执行业务落库，无权记录订单或支付最终执行结果；继续保留该表
会与两个 Pipeline 日志重复且产生职责误导。

## 9. 核心链路日志规范

稳定性链路日志统一使用英文消息和以下格式：

```text
[Component Name] <state icon> Outcome and current state. key=value
```

- `✅`：操作已经完成、状态已经恢复或幂等跳过已安全完成。
- `🔄`：正在等待、重试、降级或仍可自动恢复；消息必须说明当前保留状态或下一步。
- `❌`：本次操作失败；消息必须说明消息留在 PEL、event 保持未ACK、游标保留等实际后果。
- 高频逐事件成功使用 `DEBUG`；状态切换和批次结果使用 `INFO`；等待自动恢复使用 `WARN`；需要人工关注、
  数据链路中断或审计落库失败使用 `ERROR`。
- 组件名固定使用 `[Circuit Breaker]`、`[Redis Stream]`、`[Scheduled Redelivery]`、`[Backlog Recovery]`、
  `[Stream Consumer]`、`[PEL Recovery]`、`[Ingress ACK]`、`[Exhausted Event Pull]` 等，不使用类名代替业务组件名。

## 10. 主要设计文档

- `docs/design/event-version-design.md`：事件版本、ACK、补发和熔断。
- `docs/design/pipeline-design.md`：Pipeline 消费、分片、幂等和主动拉取。
- `docs/fuiou/design-payment.md`：富友支付字段、校验、跨年退款和账户落库。
- `docs/fuiou/api.md`：富友接口原始说明。

## 11. 当前待确认事项

- 富友支付文档只定义字符串 `paySsn`，没有独立消息版本；当前临时使用报文 `payTm` 按 `Asia/Shanghai`
  转换的 epoch milliseconds。后续仍需向富友确认独立、单调的消息版本来源，不能使用接收时间、event ID 或 Hash 代替。
- 订单 `orderDetailInfos` 当前按完整快照处理；如果上游改为增量子表，需要改成按明细版本合并。
