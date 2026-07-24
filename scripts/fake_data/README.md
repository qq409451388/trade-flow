# 富友假数据生成工具

## 概述

基于 Python 的富友回调假数据生成器，生成符合富友 JSON 格式的订单和支付报文，通过 HTTP POST 调用 Ingress 接口走完整业务链路。

## 文件结构

```
scripts/fake_data/
├── config.py   # 配置文件（商户、商品、分布比例等）
└── main.py     # 主脚本（生成器 + HTTP 客户端）

docs/sql/
└── trade-fake-databases.sql  # Fake 库初始化 SQL
```

## 快速开始

### 1. 创建 Fake 数据库

```bash
mysql -u root -p < docs/sql/trade-fake-databases.sql
```

这会创建 `trade_flow_fake` 和 `trade_pipeline_fake` 两个库，包含完整的分表结构。

### 2. 修改应用配置指向 Fake 库

修改 Ingress 的 `application-dev.yml`（或新建 `application-fake.yml`）：

```yaml
spring:
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/trade_flow_fake
    username: root
    password:
trade:
  storage:
    datasource:
      url: ${spring.datasource.url}
      username: ${spring.datasource.username}
      password: ${spring.datasource.password}
```

修改 Pipeline 的 `application-dev.yml`：

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/trade_pipeline_fake
    username: root
    password:
trade:
  storage:
    datasource:
      url: jdbc:mysql://localhost:3306/trade_flow_fake
      username: root
      password:
      hikari:
        read-only: true
```

### 3. 启动 Ingress 和 Pipeline 服务

用修改后的配置启动两个服务，确保它们连接到 Fake 数据库。

### 4. 安装 Python 依赖

```bash
pip3 install aiohttp --break-system-packages
```

### 5. 运行脚本

```bash
# 默认 15 万订单
cd scripts/fake_data
python3 main.py

# 小批量测试
python3 main.py --orders 100 --concurrency 5

# 自定义日期和数量
python3 main.py --orders 50000 --date 2026-07-23

# 仅生成 JSON 文件（不发送）
python3 main.py --orders 10000 --only-generate --output-dir ./output
```

## 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--orders` | 订单数量 | 150000 |
| `--date` | 数据日期 (YYYY-MM-DD) | 2026-07-24 |
| `--concurrency` | 异步并发数 | 20 |
| `--batch-size` | 每批发送条数 | 500 |
| `--only-generate` | 仅生成 JSON 文件不发送 | false |
| `--output-dir` | JSON 文件输出目录 | /tmp/fake_data_<日期> |

## 数据链路

```
Python脚本
  │
  ├── POST /order/store-push  ──→ Ingress ──→ trade_flow_fake
  │   (订单JSON + MD5签名)        验签         ├── trade_storage_XX (原文)
  │                               写Storage    ├── trade_storage_blob_XX (原文内容)
  │                               写Event      └── trade_order_event (事件)
  │                               发Redis
  │
  └── POST /payment/store-push ──→ Ingress ──→ trade_flow_fake
      (支付JSON + MD5签名)         验签         ├── trade_storage_XX (原文)
                                   写Storage    ├── trade_storage_blob_XX (原文内容)
                                   写Event      └── trade_payment_event (事件)
                                   发Redis
                                                 ↓ Redis Stream
                                              Pipeline ──→ trade_pipeline_fake
                                               解析Storage ├── oms_order_2026
                                               写业务表     ├── oms_order_item_2026_XX
                                                           ├── oms_payment_2026
                                                           └── pipeline_*_event_log
```

## 数据分布策略

| 维度 | 分布 |
|------|------|
| 订单类型 | 堂食 60%、外卖 30%、收银机 10% |
| 支付状态 | 仅支付 85%、部分退款 10%、全额退款 5% |
| 时间分布 | 午餐高峰 11:00-13:00 (35%)、晚餐 17:00-20:00 (35%) |
| 商品数量 | 1-2个 35%、3-4个 45%、5+个 20% |
| 商户数 | 5 个商户，每商户 2-4 个门店 |
| 商品库 | 27 种商品（热菜/凉菜/主食/饮品/火锅/茶点） |

## 签名算法

与 `FuiouSignUtils.verifySign` 完全一致：

```
1. 从 JSON 中提取 keySign 字段值
2. 从 JSON 字符串中删除 keySign 字段（包括相邻逗号）
3. 计算 MD5(secret + 删除keySign后的JSON字符串)
4. 比较计算结果与 keySign 值（忽略大小写）
```

密钥与 `application-dev.yml` 中 `trade.thirdparty.fuiou.secret` 一致：`rFXFhj8Z6GA96NQDqgg3N4djE0Dp54nj`

## 数据量估算

150k 订单 + ~172k 支付（含退款），预计写入行数：

| 表 | 行数 |
|----|------|
| trade_storage + trade_storage_blob | ~322k 行 |
| trade_order_event + trade_payment_event | ~322k 行 |
| oms_order | ~150k 行 |
| oms_order_item | ~450k 行（平均 3 商品） |
| oms_payment | ~172k 行 |
| oms_payment_account | ~172k 行 |
| pipeline_*_event_log | ~322k 行（审计日志） |

总数据量约 190 万行。预计脚本执行时间约 5-10 分钟（取决于 Ingress 处理速度）。

## 注意事项

### context-path
`trade-ingress` 的 `application.yml` 配置了 `server.servlet.context-path=/trade-ingress`，因此 `config.py` 中的 `INGRESS_BASE_URL` 必须为 `http://localhost:8115/trade-ingress`（已默认配置）。若你在 `application.yml` 中删除了 context-path，需同步修改此处。

### 重复运行与 orderNo 冲突
`generate_unique_order_no()` 使用「日期前缀 + 进程内自增序号」生成订单号。同一天重复运行脚本会生成相同的 orderNo，Ingress 侧因为订单 eventKey 取自 `keySign`（每条报文 MD5 不同）不会去重，但 Pipeline 写 `oms_order` 时会命中 `uk_order_no` 唯一约束导致失败。因此：
- 每个日期只运行一次；需要重跑时请先清空当天数据，或用 `--date` 指定不同日期。

### 时区
脚本使用 `datetime.fromtimestamp()` 将毫秒时间戳转为字符串，依赖 Python 进程的本地时区。请确保运行机器时区为 `Asia/Shanghai`（与 Java 侧 `trade.thirdparty.fuiou.payment-payload.zone-id` 一致），否则支付报文 `payTm` 的版本号会与真实时间偏移（不影响验签和入库，仅影响数据真实性）。

### Fake 库 SQL 不可重复执行
`docs/sql/trade-fake-databases.sql` 中模板表使用 `CREATE TABLE`（非 `IF NOT EXISTS`），重复执行会报 "Table already exists"。这是一次性初始化脚本；如需重建，先 `DROP DATABASE` 再执行。
