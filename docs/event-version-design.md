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
`acked=0 AND create_time <= 当前时间-15分钟` 的记录并再次发布。多 Ingress 实例通过 Redis 锁避免同时执行扫描任务；Redis
Stream 仍是至少一次投递，Pipeline 必须按 `contentType + eventId` 幂等接管。

Pipeline 接管后调用：

```http
POST /trade-ingress/event/ack
Content-Type: application/json

{"contentType":1,"eventId":123}
```

ACK 更新是幂等操作。`acked=1` 只表示 Pipeline 已可靠接管，不表示执行成功；Pipeline 执行成功、失败和重试状态均不写回
`trade_order_event` 或 `trade_payment_event`。

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

如果实际报文为嵌套结构，例如 `data.orderNo`，配置为 `/data/orderNo` 即可。缺少事件键、缺少版本、版本不是非负整数时拒绝该请求，并且不会写入 Storage。

## 消费端约束

Redis Stream 消息增加 `eventKey` 和 `messageVersion`。消费端应按
`sourceSystem + eventKey` 保存已处理最大版本，并忽略不大于该版本的乱序消息；eventId 不能代替第三方消息版本。
