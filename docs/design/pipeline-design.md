# Trade Pipeline 设计

## 1. 当前范围

当前只实现单机 Java + MySQL 下的订单持久化基础设施。Pipeline 使用自己的
`trade_pipeline` 数据库和 ShardingSphere 数据源；读取原始报文时继续通过
`StorageReader` 使用独立的只读 Storage 数据源。两套数据源、事务管理器和分片规则不得混用。

当前已实现订单和支付 Redis Stream 消费、跨 consumer PEL 回收、Storage 原文读取、结构化业务落库、
独立处理审计、Ingress ACK 和投递未 ACK 事件主动拉取。

## 2. 订单事件处理链路

1. 使用消费组从 `stream:order-event` 读取新消息。
2. 校验 `eventId/storageId/storageSha256/sourceSystem/contentType/messageVersion`。
3. 通过 `(storageId, storageSha256)` 从 Storage 读取元数据和原始 JSON，并再次校验来源与类型。
4. 校验事件 `messageVersion` 必须等于原文 `recUpdTm`。
5. 每个事件使用独立的 `pipelineTransactionManager` 事务更新订单快照并全量替换子表。
6. Pipeline 事务提交后可靠记录处理审计，再调用 Ingress `/event/ack`。
7. 业务成功但 Ingress ACK 失败时仍执行 Redis `XACK`；Ingress 保持 `acked=0`，由定时补拉仅补 ACK。
8. 已明确捕获的业务失败使用独立事务记录失败流水，随后执行 Redis `XACK`，不 ACK Ingress，等待下一轮未 ACK 补拉。
   失败流水无法落库时保留 PEL，防止静默丢失。
9. 定时通过 `XPENDING + XCLAIM` 接管超过 `pending-min-idle` 的消息，包含其他宕机 consumer 的 PEL；PEL
   只承担处理中断恢复，不承担无限业务重试。

重复或乱序事件以 `recUpdTm` 为准：小于或等于当前 `source_update_time` 的版本不再改表，
但仍视为已经接管，可以完成 Ingress ACK 和 Redis XACK。

## 3. 订单逻辑表与物理表

| 逻辑表 | 分片键 | 物理表格式 |
| --- | --- | --- |
| `oms_order` | `order_create_time` 年份 | `oms_order_YYYY` |
| `oms_order_item` | `item_create_time` 年份、`order_no` | `oms_order_item_YYYY_00` ～ `_15` |
| `oms_order_item_spec` | 父订单年份 Hint | `oms_order_item_spec_YYYY` |
| `oms_order_package_item` | `item_create_time` 年份 | `oms_order_package_item_YYYY` |
| `oms_payment` | 强制 `routeYear` Hint | `oms_payment_YYYY` |
| `oms_payment_account` | 与支付主表相同的强制 Hint | `oms_payment_account_YYYY` |

订单商品明细 Hash 使用 `Math.floorMod(order_no, 16)`，负数也能稳定落到 `00`～`15`。
规格表本身没有业务时间字段，任何新增、修改、按主键查询都必须使用
`OrderItemSpecShardingHint` 显式传入父订单年份；不带 Hint 的查询会扫描所有已配置年份，
不带 Hint 的写入会因多路由而失败。

## 4. 年份管理

`trade.pipeline.sharding.years` 只允许登记已经存在物理表的年份。当前已提前创建并登记
`2026`、`2027`。
新增年份的顺序固定为：

1. 在 `../sql/trade-pipeline-base-schema.sql` 维护最终基础结构。
2. 参照 `../sql/trade-pipeline-year-shards.sql` 从基础表创建新年度物理表。
3. 为 `oms_order_item` 创建 `00`～`15` 共 16 张表。
4. 核对索引和字段一致后，将年份追加到 `trade.pipeline.sharding.years`。
5. 重启 Pipeline，使 ShardingSphere 重新加载 actual data nodes。

不得先追加配置再建表，否则对应年份请求会被路由到不存在的物理表。

## 5. 查询约束

- 订单查询应携带 `order_create_time` 的年份或时间范围。
- 商品及套餐明细查询应携带 `item_create_time`；商品明细同时携带 `order_no` 才能单分片命中。
- 规格查询必须已知父订单年份并设置 Hint。
- 只按 `order_no` 查询订单会扫描所有已登记年份，只作为幂等兜底，不作为批量查询方式。
- 跨年度查询必须设置明确的时间边界，禁止无条件全表扫描。

## 6. MyBatis-Plus 边界

DO 和 Mapper 始终绑定逻辑表名，由 ShardingSphere 改写为物理表。每张逻辑表均提供
`XxxDbService` 和 `XxxDbServiceImpl`；业务编排层优先注入 DbService，不直接依赖 Mapper。
Pipeline 业务事务显式使用 `pipelineTransactionManager`，不得将 Pipeline 写入和 Storage
读取包装成一个本地事务。

## 7. 参考库差异与待确认项

参考项目 `xppbz` 的主表 UPSERT、子表删除重建思路被保留，但修正了以下问题：

- 不再把落库失败的消息统一 ACK；失败消息留在 PEL。
- 不再只读取当前 consumer 的 Pending；允许接管宕机实例名下的 PEL。
- 不再用 `orderNo + orderState` 判断重复，避免旧状态覆盖新版本；改用 `recUpdTm`。
- 所有查询、删除和新增都携带年度及 Hash 分片条件，规格表显式使用年份 Hint。
- Pipeline 数据提交、Ingress ACK、Redis XACK 按顺序执行，后两步失败可通过幂等重试恢复。

已确认并实现：

- 不建设 Dead Letter Stream。Ingress 未 ACK event 是持久化待处理事实，Pipeline 定时批量拉取并直接处理。
- Pipeline 已明确捕获的业务处理失败应记录执行失败、XACK 当前 Redis 消息，但不 ACK Ingress，
  等待下一轮未 ACK 补拉。PEL 只用于处理中断、审计落库失败和 Redis XACK 失败。
- `pipeline_order_event_log` 每次实际处理写一条独立流水，记录Stream、PEL接管或主动拉取、处理结果、
  失败阶段、错误码、原因和耗时。失败流水无法落库时不XACK，确保该失败不会静默消失。
- `POST /order-event/pull` 可按event ID或批量主动拉取Ingress未 ACK 事件，直接读取Storage并处理，
  成功后ACK Ingress，不经过Redis。
- Pipeline 默认每分钟按订单、支付通道各启动一次未 ACK 批量排空；按 event ID 游标连续拉取，每批500条，
  单轮最多100批或10分钟，批内共享4线程有界执行器。MySQL租约按批续期以避免多实例重复执行；失败 event
  不阻塞同一轮的后续 event，作为Redis Stream记录被误删后的最终自动恢复路径。
- 主动拉取连续失败默认最多3次。前两次保持Ingress未ACK；第三次在失败审计可靠落库后ACK Ingress，形成
  `process_status=FAILED + ingress_ack_status=SUCCEEDED` 的人工处理终态，并按批汇总发送企业微信告警。
- Pipeline 提供 `/readiness/event-consumer` 监控 Pipeline 数据库、Storage 数据源、Redis、consumer group 和订阅状态；
  readiness 不参与 Ingress 发布状态或数据正确性。

支付事件采用相同的可靠投递闭环：`stream:payment-event` + `trade-pipeline-payment` 消费组，处理结果写入
`pipeline_payment_event_log`。支付流水以 `paySsn` 为不可变幂等键：相同 SHA 为幂等成功，不同 SHA 记为
`PAYMENT_PAYLOAD_CONFLICT` 且不覆盖原记录。支付主表和结算账户通过同一个强制年份 Hint 路由并在单事务提交；
支付通道可通过 `POST /payment-event/pull` 主动拉取 Ingress 未 ACK 事件。

仍待业务确认：

- 当前把富友 `orderDetailInfos` 视为完整快照，所以新版本会全量替换明细、规格和套餐。
  如果第三方将来发送增量子表，必须改为按明细版本合并。
- 当前约定任何内容变化都会推进 `recUpdTm`；相同 `recUpdTm` 的不同内容会被视为重复版本。
