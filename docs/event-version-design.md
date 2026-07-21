# Event 消息版本设计

> 更新日期：2026-07-21。当前实现面向单机 Java + 单台 MySQL。

## 数据契约

`trade_order_event` 和 `trade_payment_event` 增加 `message_version BIGINT UNSIGNED NOT NULL`。
消息版本由第三方来源 adapter 从原始报文中提取并转换为非负 `long`，业务 Service 不解析第三方 JSON。

版本幂等键：

- 订单：`source_system + third_event_key + message_version`
- 支付：`source_system + third_event_key + message_version`

相同事件键、相同版本重复推送不会生成第二条 event。为保证至少一次投递，未 ACK 的 event 允许重复发布到 Redis Stream。

## 入库行为

ingress 固定保存并发布所有版本，不提供版本保留策略配置。每个新版本保存一条 event，消费端根据
`eventKey + messageVersion` 决定是否处理。相同业务事件的旧版本晚到时仍会入库和发布，ingress 不判断版本新旧。

相同事件键、相同版本的重复推送由数据库唯一键兜底，不产生第二条 event。

## Ingress 投递与 ACK

event 表只表示 Ingress 是否已将事件可靠移交给 Pipeline：

- `acked=0`：event 已在 Ingress 入库，Pipeline 尚未确认接管。
- `acked=1`：Pipeline 已将事件持久化到自己的 Inbox、任务表或执行表，并向 Ingress ACK。

Ingress 在 event 事务提交后立即发布一次，并在第 1、5、10 分钟检查 ACK 状态后进行短期补发。定时任务每15分钟扫描
`acked=0 AND auto_redelivery_count<2 AND create_time<=当前时间-15分钟` 的记录并再次发布。只有成功写入 Redis Stream
才增加自动补发次数，短期重试不计数。达到2次后停止自动扫描，等待人工处理。多 Ingress 实例通过 Redis 锁避免同时执行扫描任务；Redis
Stream 仍是至少一次投递，Pipeline 必须按 `contentType + eventId` 幂等接管。

Pipeline 接管后调用：

```http
POST /trade-ingress/event/ack
Content-Type: application/json

{"contentType":1,"eventId":123}
```

ACK 更新是幂等操作。`acked=1` 只表示 Pipeline 已可靠接管，不表示执行成功；Pipeline 执行成功、失败和重试状态均不写回
`trade_order_event` 或 `trade_payment_event`。

自动补发耗尽记录及人工处理接口：

```text
GET  /trade-ingress/event/redelivery-exhausted?contentType=1&limit=100
POST /trade-ingress/event/redeliver
POST /trade-ingress/event/resume-redelivery
```

`redeliver` 只人工发布一次，不修改已耗尽次数；`resume-redelivery` 将次数清零，使记录重新进入下一轮自动扫描。即使已经耗尽，
迟到的 Pipeline ACK 仍可正常将记录更新为 `acked=1`。

人工恢复以 Pipeline 主动拉取耗尽事件并直接处理为主，不经过 Redis；Ingress 的 `redeliver` 和
`resume-redelivery` 接口保留为备用恢复通道。总自动投递机会由首次投递、`retry-delays` 和
`max-auto-redeliveries` 共同确定，不再单独配置 `max-attempts`。当前默认共6次。

Pipeline 主动拉取使用：

```http
POST /trade-pipeline/order-event/pull
```

可指定 `eventIds`，或通过 `limit` 拉取最旧的订单耗尽事件。Pipeline 直接读取 Storage、处理并在成功后
ACK Ingress，不重新写入 Redis。Ingress 的耗尽事件查询会同时返回 `storageId + storageSha256`。

## Redis 发布熔断

熔断状态按事件通道持久化在 MySQL `trade_event_delivery_control`，Java 内存只缓存最多5秒，Redis 不参与
保存自身熔断状态。服务重启后必须从数据库恢复状态。状态不变量如下：

- `CLOSED`：普通请求、短期重试和定时扫描允许发布。
- `OPEN`：所有自动发布立即跳过；Storage 和 event 仍正常入库，不安排新的短期重试，也不增加自动补发次数。
- `HALF_OPEN`：普通发布仍禁止，只有持有 MySQL 恢复租约的实例可以执行最多10条试投。

状态流转：

1. `CLOSED -> OPEN`：1分钟窗口内 Redis 发布失败达到10次。Redis扫描任务锁不可用也计为发布基础设施失败。
2. `OPEN -> OPEN`：每30秒检查 Ingress Redis PING 和 Pipeline readiness；任一失败则清零连续健康次数。
3. `OPEN -> HALF_OPEN`：上述两项连续2次健康，且当前实例取得数据库恢复租约。
4. `HALF_OPEN -> OPEN`：任一试投失败，立即重新打开并等待下一轮探活。
5. `HALF_OPEN -> CLOSED`：试投全部成功；记录当时最大未ACK event ID作为本轮积压截止点。
6. `CLOSED` 积压恢复：持有恢复租约的实例按 ID 游标每批最多100条恢复，每条重新执行立即投递并安排
   1、5、10分钟短期重试。新进入的事件不受积压游标影响，正常实时发布。

`recovery_owner + recovery_lease_until` 保证多 Ingress 实例只有一个探活或恢复；
`recovery_cursor_id + recovery_cutoff_id` 保证重启后继续上次积压进度，不会一次性灌入全部消息。
Ingress 人工 `redeliver` 是明确的运维强制通道，允许绕过 `OPEN` 自动发布限制，但失败仍会更新熔断失败信息；
它不会自行关闭熔断。

Pipeline readiness 不是固定返回UP的存活检查，而是验证 Pipeline数据库、Storage只读数据源、Redis、
订单消费组以及内容类型支持情况。支付consumer尚未实现，因此支付通道readiness当前不会返回可恢复。

## 富友字段提取

富友订单适配器通过 Jackson JSON Pointer 提取字段：

```yaml
trade:
  thirdparty:
    fuiou:
      order-payload:
        event-key-pointer: /orderNo
        message-version-pointer: /version
```

如果实际报文为嵌套结构，例如 `data.orderNo`，配置为 `/data/orderNo` 即可。Ingress 先保存原始内容，
再提取 event 字段；缺少事件键、缺少版本、版本不是非负整数或 event 持久化失败时拒绝请求，并将
`storageId + payloadSha256 + 失败阶段 + 失败原因` 写入 `trade_event_ingest_failure_log`。失败审计不重复保存原文。

## 消费端约束

Redis Stream 消息增加 `eventKey` 和 `messageVersion`。消费端应按
`sourceSystem + eventKey` 保存已处理最大版本，并忽略不大于该版本的乱序消息；eventId 不能代替第三方消息版本。
