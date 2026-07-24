# Event 版本与可靠接管设计

> 更新日期：2026-07-23。当前实现面向单机 Java、MySQL 与 Redis Stream。

## 1. 事实源和职责

- Ingress MySQL 的 `trade_order_event`、`trade_payment_event` 是事件事实源。
- Redis Stream 只负责实时通知和削峰，通知允许重复、暂停或被裁剪。
- Pipeline 处理审计是业务是否已经成功的事实。
- `event.acked=0` 表示 Pipeline 尚未完成接管确认，必须继续被定时补拉看到。
- `event.acked=1` 表示 Pipeline 已经成功处理，或失败已达到人工终态并可靠记录审计。

Ingress 写 Storage 和 event 后尝试发布 Redis 通知。Redis 失败不影响 event 入库，也不把 event 标记为失败。
Pipeline 除实时消费外，定时补拉超过实时缓冲期的全部未 ACK event，保证 Redis 丢失时仍可恢复。

## 2. 消息版本

`trade_order_event` 和 `trade_payment_event` 使用来源 adapter 提取的非负 `message_version`：

- 订单幂等键：`source_system + third_event_key + message_version`。
- 支付幂等键：`source_system + third_event_key + message_version`。
- Redis 消息携带 `eventId + storageId + storageSha256 + sourceSystem + eventKey + messageVersion`。
- 消费端按第三方 `messageVersion` 判断新旧，不使用 event ID 或 Snowflake ID 代替业务版本。

富友订单暂取 `recUpdTm`；支付暂将 `payTm` 按 `Asia/Shanghai` 转成 epoch milliseconds。

## 3. Ingress Redis 发布保护

发布保护只保护 Ingress 与 Redis，不参与 Pipeline 业务确认：

```text
CLOSED --连续发布失败--> OPEN --Redis连续健康--> CLOSED
```

- 发布有每秒速率限制、Stream 高低水位和 `MAXLEN ~` 受控长度。
- OPEN 时停止新的 XADD，Storage 和 event 继续写入。
- Redis 恢复后只恢复新通知，不重推历史 event。
- 首次通知失败后最多执行2次进程内短期重试，任一次成功即停止；不存在数据库重投次数或周期历史重推任务。
- 不存在 HALF_OPEN、真实业务探针、Pipeline readiness 驱动关闭或 Ingress 积压恢复游标。

## 4. Pipeline 实时消费

Pipeline 完成业务处理后先可靠写处理审计，再尝试 Ingress ACK，最后 Redis XACK：

| 结果 | Ingress ACK | Redis XACK |
| --- | --- | --- |
| APPLIED / IGNORED，ACK 成功 | 成功 | 执行 |
| APPLIED / IGNORED，ACK 失败 | 保持 `acked=0`，由补拉仅补 ACK | 执行 |
| 业务失败且失败审计成功 | 不 ACK | 执行 |
| 审计落库失败 | 不 ACK | 不执行，保留 PEL |
| 处理中崩溃或 XACK 失败 | 未完成或按前序结果 | 保留 PEL |

PEL 只恢复处理中断、审计落库失败与 XACK 失败，不承担 HTTP ACK 或长期业务重试。

## 5. 未 ACK 批量补拉

Ingress 内部查询接口：

```http
GET /trade-ingress/event/unacked?contentType=1&limit=500&afterEventId=0
```

核心查询：

```sql
WHERE acked = 0
  AND create_time <= :now - :realtimeGracePeriod
  AND id > :afterEventId
ORDER BY id
LIMIT :batchSize
```

索引为 `(acked, create_time, id)`。订单、支付独立调度并使用内容类型级 MySQL 租约；每批默认500条，
保留最大批次、最大运行时长、批次续租和共享有界工作线程。单条失败不阻塞同批后续事件，下一轮重新从游标0查询。

每批先按 `event_id IN (...)` 一次查询成功审计，以及主动补拉失败次数已经达到终态阈值的审计：

- 已成功或已进入人工终态：不读 Storage，不运行 Handler，仅进入批量 ACK。
- 未成功：运行对应 Handler；成功或达到人工终态后进入同一批量 ACK。
- 可恢复失败：保持未 ACK，下一轮重试。

批量 ACK 接口：

```http
POST /trade-ingress/event/batch-ack
```

单批最多500条，幂等返回请求数、首次 ACK 数、已 ACK 数和不存在数。HTTP 失败不回滚业务，下轮继续收敛。

## 6. 并发与终态

- 2分钟实时缓冲期降低实时 Consumer 短时积压时与补拉同时处理同一事件的概率。
- 多 Pipeline 实例通过 `pipeline_event_pull_control` 保证每个内容类型只有一个补拉 sweep。
- 订单更新使用带版本条件的原子 UPDATE；竞争失败的旧版本按 IGNORED 收敛，不能覆盖新版本。
- 新订单不执行空范围子表 DELETE；订单更新先解析旧记录主键，再携带主键和分片条件精确删除，避免
  `order_no` 范围 DELETE 的 InnoDB gap lock。瞬时死锁会在全新事务中有限重试。
- 支付通过 `paySsn` 唯一约束和 SHA 比较保证不可变幂等。
- 主动补拉连续失败达到阈值后，先可靠写失败审计，再批量 ACK 形成人工终态，并按批汇总企微告警。
- 审计、失败次数查询或 ACK 失败时保持可恢复状态。

Readiness 仅用于监控 Pipeline DB、Storage、Redis、消费组和 Subscription 状态，不参与数据正确性或 Ingress 发布状态流转。
