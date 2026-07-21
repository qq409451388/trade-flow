# Event 消息版本设计

> 更新日期：2026-07-21。当前实现面向单机 Java + 单台 MySQL。

## 数据契约

`trade_order_event` 和 `trade_payment_event` 增加 `message_version BIGINT UNSIGNED NOT NULL`。
消息版本由第三方来源 adapter 从原始报文中提取并转换为非负 `long`，业务 Service 不解析第三方 JSON。

版本幂等键：

- 订单：`source_system + third_event_key + message_version`
- 支付：`source_system + event_key + message_version`

相同事件键、相同版本重复推送不会生成第二条 event，也不会重复发布到 Redis Stream。

## 入库行为

receiver 固定保存并发布所有版本，不提供版本保留策略配置。每个新版本保存一条 event，消费端根据
`eventKey + messageVersion` 决定是否处理。相同业务事件的旧版本晚到时仍会入库和发布，receiver 不判断版本新旧。

相同事件键、相同版本的重复推送由数据库唯一键兜底：不产生第二条 event，也不重复发布。

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
