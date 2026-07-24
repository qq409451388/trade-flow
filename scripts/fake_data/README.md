# 富友假数据生成工具 (亿级支持)

## 概述

基于 Python 的富友回调假数据生成器，生成符合富友 JSON 格式的订单和支付报文，通过 HTTP POST 调用 Ingress 接口走完整业务链路。

支持**亿级数据量**的流式生成与发送，可指定虚拟时间范围模拟分表分库逻辑。

## 文件结构

```
scripts/fake_data/
├── config.py          # 配置文件（商户、商品、分布比例、性能参数）
├── main.py            # 主脚本（流式生成器 + HTTP 客户端 + 断点续传）
├── README.md          # 本文档
├── progress.json      # 运行时自动生成，记录断点续传进度
└── errors.jsonl       # 运行时自动生成，记录发送失败的请求

docs/sql/
└── trade-fake-databases.sql  # Fake 库初始化 SQL
```

## 快速开始

### 1. 创建 Fake 数据库

```bash
mysql -u root -p < docs/sql/trade-fake-databases.sql
```

### 2. 修改应用配置指向 Fake 库

Ingress `application-dev.yml`：

```yaml
spring:
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/trade_flow_fake
    username: root
    password: <your_password>
trade:
  storage:
    datasource:
      url: ${spring.datasource.url}
      username: ${spring.datasource.username}
      password: ${spring.datasource.password}
```

Pipeline `application-dev.yml`：

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/trade_pipeline_fake
    username: root
    password: <your_password>
trade:
  storage:
    datasource:
      url: jdbc:mysql://localhost:3306/trade_flow_fake
      username: root
      password: <your_password>
```

### 3. 安装依赖

```bash
pip3 install aiohttp --break-system-packages
```

### 4. 启动服务

确保 Ingress 和 Pipeline 服务已启动并连接到 Fake 数据库。

### 5. 运行脚本

```bash
cd scripts/fake_data

# 小批量验证 (100 条，单日)
python3 main.py --total-orders 100 --start-date 2026-07-24 --end-date 2026-07-24

# 15 万订单，单日 (原有默认量级)
python3 main.py --total-orders 150000 --start-date 2026-07-24 --end-date 2026-07-24

# 1 亿订单，分布在 2025-01-01 ~ 2025-06-30 (约 55 万/天)
python3 main.py --total-orders 100000000 --start-date 2025-01-01 --end-date 2025-06-30

# 指定每日 50 万，跨年测试分表
python3 main.py --orders-per-day 500000 --start-date 2024-12-15 --end-date 2025-01-15

# 断点续传 (中断后继续)
python3 main.py --total-orders 100000000 --start-date 2025-01-01 --end-date 2025-06-30 --resume

# 限速 1000 条/秒 (避免压垮服务端)
python3 main.py --total-orders 1000000 --start-date 2025-01-01 --end-date 2025-01-07 --rate-limit 1000

# 仅生成 JSONL 文件 (不发送，适合验证数据格式)
python3 main.py --total-orders 1000 --start-date 2026-07-24 --end-date 2026-07-24 --only-generate
```

## 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--total-orders` | 订单总量，自动按天数均摊 | 无 (与 `--orders-per-day` 二选一) |
| `--orders-per-day` | 每日订单量 | 550000 |
| `--start-date` | 起始日期 (YYYY-MM-DD) | 2025-01-01 |
| `--end-date` | 结束日期 (YYYY-MM-DD) | 2025-06-30 |
| `--concurrency` | HTTP 并发数 | 50 |
| `--batch-size` | 每批生成+发送的订单数 | 1000 |
| `--rate-limit` | 全局限速 (条/秒)，0=不限速 | 0 |
| `--only-generate` | 仅生成 JSONL 文件不发送 | false |
| `--output-dir` | JSONL 输出目录 (仅 `--only-generate`) | ./fake_data_\<start\>_\<end\> |
| `--resume` | 从 progress.json 断点续传 | false |
| `--progress-file` | 进度文件路径 | progress.json |
| `--error-file` | 错误记录文件路径 | errors.jsonl |

## 核心设计

### 流式处理 (内存恒定)

```
循环: 生成 batch_size 条 → 签名 → 发送 → 丢弃
```

内存占用 = O(batch_size)，与总数据量无关。1 亿订单与 1 万订单的内存占用相同 (约 50MB)。

### 日期范围与分表分库

- 订单按 `--start-date` 到 `--end-date` 逐日生成
- 每天的订单号前缀为 `YYYYMMDD * 10_000_000 + 日内序号`，跨日天然唯一
- 跨年日期范围 (如 2024-12-15 ~ 2025-01-15) 会触发 Pipeline 的年度分表路由 (`oms_order_2024` / `oms_order_2025`)
- Storage 100 分片由 `payload_sha256 % 100` 决定，数据天然分散到 `_00 ~ _99`

### 断点续传

- 每 10 批自动保存进度到 `progress.json`
- Ctrl+C 中断时自动保存
- `--resume` 跳过已完成日期，从中断处继续
- 单条请求失败自动重试 3 次，最终失败记录到 `errors.jsonl`

## 服务端调优 (亿级压测)

### Ingress 调优

`application.yml` 关键参数：

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 50        # 默认 10 太小，亿级压测建议 50+
      minimum-idle: 10
  data:
    redis:
      lettuce:
        pool:
          max-active: 32           # 默认 16，建议增大
          max-idle: 16

trade:
  stream:
    publish-rate-per-second: 5000  # 默认 2000，亿级建议 5000+
    max-length: 5000000            # 默认 100万，建议 500万
    high-watermark: 4500000
    low-watermark: 3500000
```

JVM 参数 (启动脚本)：

```bat
set JVM_OPTS=-Xmx2g -Xms2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200
```

### Pipeline 调优

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 50
      minimum-idle: 10
```

JVM 参数：

```bat
set JVM_OPTS=-Xmx2g -Xms2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200
```

### MySQL 调优

```sql
-- my.cnf 关键参数
innodb_buffer_pool_size = 4G       -- 至少可用内存的 50%
innodb_log_file_size = 1G
innodb_flush_log_at_trx_commit = 2 -- 压测可接受，生产用 1
max_connections = 200
bulk_insert_buffer_size = 256M
```

### Redis 调优

```conf
maxmemory 2gb
maxmemory-policy noeviction        -- Stream 不淘汰
```

### 吞吐量估算

| 并发数 | 预期吞吐 | 1 亿订单耗时 |
|--------|----------|-------------|
| 50 | ~2000/s | ~14 小时 |
| 100 | ~3000/s | ~9 小时 |
| 200 | ~5000/s | ~5.5 小时 |

> 实际吞吐受 Ingress 的 `publish-rate-per-second` 限制。提升该值需确保 MySQL 和 Redis 能跟上。

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
                                               解析Storage ├── oms_order_YYYY (年度分表)
                                               写业务表     ├── oms_order_item_YYYY_XX (年度+100分片)
                                                           ├── oms_payment_YYYY (年度分表)
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

密钥与 `application-dev.yml` 中 `trade.thirdparty.fuiou.secret` 一致。

## 注意事项

### context-path
`trade-ingress` 的 `application.yml` 配置了 `server.servlet.context-path=/trade-ingress`，因此 `config.py` 中的 `INGRESS_BASE_URL` 必须为 `http://localhost:8115/trade-ingress`。

### 重复运行与 orderNo 冲突
订单号格式为 `YYYYMMDD * 10_000_000 + 日内序号`。同一天重复运行会生成相同的 orderNo，Pipeline 写 `oms_order` 时命中 `uk_order_no` 唯一约束失败。因此：
- 每个日期只运行一次
- 需要重跑时先清空当天数据，或用不同日期范围
- 断点续传 (`--resume`) 不会重复发送已完成的订单

### 时区
脚本依赖 Python 进程的本地时区。请确保运行机器时区为 `Asia/Shanghai`，否则时间字段会偏移。

### Fake 库 SQL 不可重复执行
`docs/sql/trade-fake-databases.sql` 使用 `CREATE TABLE`（非 `IF NOT EXISTS`），重复执行会报错。如需重建，先 `DROP DATABASE` 再执行。

### 磁盘空间
1 亿订单 + 支付数据预计占用 MySQL 约 200-300GB 磁盘空间。请确保有足够空间。
