# 支付流水落库与分表开发规则

## 1. 目标

实现支付推送的解析、校验、幂等落库、年度分表和结算账户明细落库。

逻辑表：

- `oms_payment`
- `oms_payment_account`

物理表：

- `oms_payment_YYYY`
- `oms_payment_account_YYYY`

所有年度物理表复用相同表结构。业务代码、Mapper 和 DO 只能使用逻辑表名，禁止在业务代码中拼接物理表名。物理路由由分片组件统一完成。

---

## 2. 数据来源

支付业务表只保存结构化业务字段。

原始 JSON、签名和未识别扩展字段由 Storage 保存，业务表仅保存：

- `storage_id`
- `payload_sha256`

处理顺序：

1. 根据事件中的 `StorageRef` 读取原始报文。
2. 校验事件与 Storage 元数据中的来源、内容类型和 SHA-256 一致；签名已由 Ingress 在接收时校验。
3. 解析 JSON。
4. 校验必要字段。
5. 计算分片年份。
6. 在同一数据库事务中写入支付流水和账户明细。
7. 提交成功后 ACK Pipeline 事件。

支付接口中 `paySsn` 是交易流水号，`payState` 只有支付成功和退款成功，退款通过 `sourcePaySsn` 指向原支付流水；结算账户通过 `acntNoInfoList` 列表返回。fileciteturn4file0

---

## 3. `oms_payment` 字段规则

### 3.1 系统字段

- `id`：雪花 ID。
- `storage_id`：原始报文 Storage ID。
- `payload_sha256`：原始报文 SHA-256。
- `create_time`：首次落库时间。
- `update_time`：更新时间。

### 3.2 流水标识

- `pay_ssn`：映射 `paySsn`，不能为空，是支付流水业务唯一键。
- `source_pay_ssn`：映射 `sourcePaySsn`。
    - 支付流水为空字符串。
    - 退款流水必须有值。
- `order_no`：映射 `orderNo`。
    - 允许为 `0`。
    - `0` 表示充值等非普通订单支付。
    - 不允许用 `order_no` 作为支付唯一键。

### 3.3 商户和门店

- `mchnt_cd` ← `mchntCd`
- `shop_id` ← `shopId`
- `shop_name` ← `shopName`

`shop_id=0` 是合法业务值，不能按脏数据过滤。

### 3.4 时间

- `pay_time` ← `payTm`
- `refund_time` ← `refundTm`

规则：

- 使用固定业务时区 `Asia/Shanghai`。
- 禁止使用 JVM 或服务器默认时区。
- 接口时间格式按 `yyyy-MM-dd HH:mm:ss` 严格解析。
- 数据库存储为 `DATETIME(3)`。
- 支付流水的 `refund_time` 为 `NULL`。
- 退款流水必须存在 `refund_time`。
- `pay_time` 是分片键。

### 3.5 支付状态和渠道

- `pay_type` ← `payType`
- `pay_name` ← `payName`
- `pay_state` ← `payState`
    - `1`：支付成功
    - `2`：退款成功
- `fy_settle` ← `fySettle`

`pay_type` 和 `pay_name` 必须同时保留。同一个支付编码可能出现不同名称，禁止通过编码自动覆盖接口名称。

### 3.6 金额

以下字段统一使用有符号 `BIGINT`，单位为分：

- `pay_amt` ← `payAmt`
- `fee_amt` ← `feeAmt`
- `refund_amt` ← `refundAmt`
- `balance_dis_amt` ← `balanceDisAmt`
- `face_amt` ← `faceAmt`

不能增加金额大于等于零的数据库约束，实际数据中存在负数金额。

### 3.7 第三方业务字段

- `third_order_no` ← `thirdOrderNo`
- `channel_trade_no` ← `channelTradeNo`
- `big_category` ← `bigCategory`
- `small_category` ← `smallCategory`
- `order_source` ← `orderSource`

规则：

- `third_order_no` 不是唯一键，同一个第三方订单可能对应多个支付和退款流水。
- `channel_trade_no` 当前数据唯一，但接口没有承诺绝对唯一，只建立普通索引。
- 字符串 `"0"`、空字符串保持接口原值，不擅自转成 `NULL`。

### 3.8 会员快照

- `member_name` ← `memberName`
- `phone` ← `phone`
- `member_card_no` ← `memberCardNo`
- `member_level` ← `memberLevel`

这些字段是支付发生时的快照，不能通过会员主数据反向覆盖。

### 3.9 实际报文扩展字段

以下字段虽然未在支付接口文档中定义，但实际报文稳定存在，按可选扩展字段解析：

- `open_id` ← `openId`
- `order_type` ← `orderType`
- `channel_type` ← `channelType`

要求：

- 字段缺失不能导致解析失败。
- 字段出现时正常落库。
- 不参与核心必填校验。
- 新增未知 JSON 字段不能导致反序列化失败。

---

## 4. 不进入支付业务表的字段

以下字段只保存在原始 Storage：

- `payMsg`
    - 实际数据完全由 `payState` 推导。
- `keySign`
    - 只用于验签。
- 原始 `pushBody`
    - 不重复保存到业务表。
- `reserve1`
    - 预留字段，业务语义不明确。
- `shopSettleType`
    - 支付接口文档未定义。
- `orderInfoType`
    - 支付接口文档未定义。
- 其他未来新增但未建模的扩展字段。

禁止因为业务表没有对应字段而丢弃原始报文。

---

## 5. `oms_payment_account` 落库规则

`acntNoInfoList` 是列表，关系为：

```text
oms_payment 1 -> 0..N oms_payment_account
```

每个账户元素生成一条记录。

字段：

- `id`：雪花 ID。
- `payment_id`：关联 `oms_payment.id`。
- `pay_ssn`：冗余保存支付流水号，方便查询。
- `account_seq`：元素在 `acntNoInfoList` 中的顺序，从 `0` 开始。
- `cust_acnt_tp` ← `custAcntTp`
- `mchnt_cd` ← `mchntCd`
- `out_acnt_nm` ← `outAcntNm`
- `out_acnt_no` ← `outAcntNo`
- `shop_id` ← `shopId`
- `create_time`
- `update_time`

规则：

- `outAcntNo` 按接口原值保存，接口返回的是加密卡号。
- 不能假设一个 `pay_ssn` 永远只有一个账户。
- 唯一约束语义为 `payment_id + account_seq`。
- 不建立数据库外键，由应用事务维护一致性。
- 账户表必须与支付主表落在同一个年度分片。

支付主表和账户明细必须在同一事务中提交，禁止出现只有账户没有支付主表，或支付主表成功但账户只写入一部分的情况。

当前实现通过同一个强制年度 Hint 同时路由 `oms_payment` 和 `oms_payment_account`。支付表操作缺少
`routeYear` 时立即失败，禁止 ShardingSphere 广播到全部年度表。

---

## 6. 年度分表规则

### 6.1 基础路由

统一使用：

```text
routeYear = year(pay_time)
```

适用于：

- 支付成功流水。
- 退款成功流水。
- 普通订单支付。
- `order_no=0` 的充值等支付。

禁止使用以下时间作为主分片键：

- 回调接收时间。
- 本地创建时间。
- `refund_time`。

支付推送可能延迟约一小时，因此接收时间可能已经跨天甚至跨年，不能代表业务所属年份。fileciteturn4file0

### 6.2 退款路由

正常情况下，退款报文中的 `payTm` 表示原支付时间，因此：

```text
退款分片年份 = year(payTm)
```

不允许每次退款都先通过 `sourcePaySsn` 查询原支付，这会增加一次数据库查询并放大写入成本。

### 6.3 跨年窗口兜底

因为当前是按年分表，普通跨天不影响路由。只有 `12 月 31 日至 1 月 1 日` 的跨年窗口需要特殊处理。

配置：

```text
payment.sharding.year-boundary-window-hours=3
```

该值必须可配置，不得硬编码在业务逻辑中。

退款满足以下条件时才启用 `sourcePaySsn` 兜底查询：

1. `pay_state=2`；
2. 当前事件处于跨年窗口，或者 `pay_time` 与接收时间分属相邻年份；
3. `source_pay_ssn` 非空。

处理流程：

1. 根据 `pay_time` 得到候选年份 `candidateYear`。
2. 优先在 `oms_payment_candidateYear` 查询 `source_pay_ssn`。
3. 如果未找到，再查询相邻年份：
    - `candidateYear - 1`
    - `candidateYear + 1`
4. 找到原支付后，退款使用原支付所在物理表年份。
5. 原支付账户和退款账户也使用相同年份。
6. 如果所有候选表都没有找到原支付：
    - 仍按 `candidateYear` 落库；
    - 记录 `SOURCE_PAYMENT_NOT_FOUND` 告警；
    - 不阻断正常落库。

退款发生在几年后，但报文 `payTm` 仍是原支付时间的情况，不属于跨年窗口，直接按 `payTm` 路由，无须查询原支付。

---

## 7. 分片组件约束

业务层只能传递分片上下文：

```text
PaymentShardContext.routeYear
```

由统一分片组件完成：

```text
oms_payment         -> oms_payment_YYYY
oms_payment_account -> oms_payment_account_YYYY
```

要求：

- Mapper XML、Repository 和 Service 中禁止拼接 `_2026`、`_2027` 等表名。
- 支付和账户写入必须使用相同 `routeYear`。
- `routeYear` 缺失时立即失败，禁止广播写入。
- 查询已知 `pay_time` 时必须精确路由。
- 时间范围查询只路由涉及的年份。
- 不带年份的 `pay_ssn` 查询禁止默认扫描所有历史分片。
- 跨年退款的 `source_pay_ssn` 查询最多扫描候选年及相邻年份。

---

## 8. 幂等和冲突处理

业务幂等键：

```text
pay_ssn
```

同一个年度物理表中必须唯一。

处理规则：

### 首次收到

1. 生成 `payment_id`。
2. 插入 `oms_payment`。
3. 批量插入 `oms_payment_account`。
4. 提交事务。

### 重复收到且 SHA 相同

满足：

```text
existing.pay_ssn == incoming.pay_ssn
existing.payload_sha256 == incoming.payload_sha256
```

处理为幂等成功：

- 不更新支付记录。
- 不重复插入账户。
- 正常记录执行成功并 ACK。

### 重复收到但 SHA 不同

满足：

```text
existing.pay_ssn == incoming.pay_ssn
existing.payload_sha256 != incoming.payload_sha256
```

处理为业务冲突：

- 禁止覆盖原记录。
- 禁止使用后到报文静默更新金额、状态或账户。
- 记录 `PAYMENT_PAYLOAD_CONFLICT`。
- Pipeline 本次处理失败，不 ACK，进入人工或重试处理。

---

## 9. 参数校验

### 必填字段

所有流水必须满足：

- `paySsn` 非空。
- `mchntCd` 非空。
- `payTm` 格式正确。
- `payState` 为 `1` 或 `2`。

退款流水额外要求：

- `sourcePaySsn` 非空。
- `refundTm` 非空且格式正确。

### 允许的特殊值

以下情况不能判定为脏数据：

- `orderNo=0`
- `shopId=0`
- `payType=""`
- `thirdOrderNo=""`
- `thirdOrderNo="0"`
- `channelTradeNo=""`
- 会员字段为空
- `acntNoInfoList` 为空

### 异常处理

以下情况整条事件处理失败：

- JSON 无法解析。
- 必填字段缺失。
- 时间格式错误。
- `payState` 不支持。
- 账户列表元素类型错误。
- `shopId` 无法转换为数字。
- 主表或账户表事务提交失败。

禁止静默跳过异常账户元素后继续提交支付主表。

---

## 10. 索引语义

`oms_payment`：

- 唯一：`pay_ssn`
- 普通索引：
    - `source_pay_ssn`
    - `order_no`
    - `third_order_no`
    - `channel_trade_no`
    - `pay_time`
    - `mchnt_cd + shop_id + pay_time`
    - `create_time`

`oms_payment_account`：

- 唯一：`payment_id + account_seq`
- 普通索引：
    - `pay_ssn`
    - `mchnt_cd + shop_id`

禁止给以下字段加唯一约束：

- `order_no`
- `source_pay_ssn`
- `third_order_no`
- `channel_trade_no`
- 账户表的 `pay_ssn`

---

## 11. 建议代码结构

```text
payment/
├── domain/
│   ├── Payment.java
│   ├── PaymentAccount.java
│   └── PaymentState.java
├── application/
│   └── PaymentPushProcessor.java
├── parser/
│   ├── FuiouPaymentPayload.java
│   ├── FuiouPaymentAccountPayload.java
│   └── FuiouPaymentPayloadParser.java
├── converter/
│   └── FuiouPaymentConverter.java
├── repository/
│   ├── PaymentRepository.java
│   └── PaymentAccountRepository.java
├── sharding/
│   ├── PaymentShardContext.java
│   ├── PaymentShardRouter.java
│   └── PaymentYearBoundaryResolver.java
└── validation/
    └── PaymentPayloadValidator.java
```

职责：

- `Parser`：只负责兼容性 JSON 解析。
- `Validator`：执行必填和枚举校验。
- `Converter`：DTO 转领域对象，不访问数据库。
- `PaymentShardRouter`：普通 `pay_time` 年份路由。
- `PaymentYearBoundaryResolver`：仅处理跨年退款兜底查询。
- `Processor`：组织 Storage 元数据校验、原文读取、解析、分片和事务；验签属于 Ingress 接收职责。
- `Repository`：只操作逻辑表名。

---

## 12. 必须覆盖的测试

1. 普通支付按 `pay_time` 年份落库。
2. `order_no=0` 的充值支付正常落库。
3. 同年退款不查询原支付，直接按 `pay_time` 路由。
4. 跨年窗口退款能够通过 `source_pay_ssn` 找到上一年度支付。
5. 跨年窗口找不到原支付时按候选年份落库并产生告警。
6. 退款发生多年后仍按原 `pay_time` 年份落库。
7. 支付推送延迟跨年时不使用接收时间分片。
8. 相同 `pay_ssn`、相同 SHA 重放时幂等成功。
9. 相同 `pay_ssn`、不同 SHA 时产生冲突且不覆盖。
10. 空账户列表正常提交。
11. 多账户列表完整落库并保持原始顺序。
12. 支付表写入失败时账户表不得产生记录。
13. 账户中任一元素非法时整个事务回滚。
14. 未知扩展字段不导致 JSON 解析失败。
15. `openId`、`orderType`、`channelType` 缺失时正常落库。

---

## 13. 当前事件处理闭环

- Redis Stream：`stream:payment-event`，消费组 `trade-pipeline-payment`。
- Pipeline 成功落库后先写 `pipeline_payment_event_log`，再 ACK Ingress，最后 XACK Redis。
- 解析、校验、SHA 冲突或数据库失败时，独立事务记录失败流水并 XACK Redis，但不 ACK Ingress；事件在
  Ingress 达到自动投递上限后等待 Pipeline 主动拉取。
- 主动拉取接口：`POST /trade-pipeline/payment-event/pull`，支持 `eventIds` 或 `limit`。
- 就绪检查：`GET /trade-pipeline/readiness/event-consumer?contentType=2`，检查 Pipeline DB、Storage DB、
  Redis 和支付消费组；该结果参与 Ingress 支付通道熔断恢复。
- PEL 只处理进程中断以及“业务已落库但审计或 Ingress ACK 尚未完成”的恢复，不承担无限业务重试。

### 13.1 支付消息版本

富友官方文档只定义了字符串 `paySsn`，没有单独的支付消息版本字段；实际流水号可能包含字母，不能转换为
非负 `long`。当前来源 adapter 暂以报文 `payTm` 按 `Asia/Shanghai` 解析后的 epoch milliseconds 作为
`messageVersion`，`paySsn` 仅作为 `thirdEventKey`。Pipeline 必须用同一时区重新计算并校验版本一致性。

`payTm` 只有秒精度且表达交易时间，这是一项临时来源契约；如果富友后续提供独立、单调的消息版本字段，应直接
切换到该字段，禁止使用 event ID、接收时间或本地 Hash 冒充第三方版本。
