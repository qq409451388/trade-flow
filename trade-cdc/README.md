# trade-cdc

交易 CDC 同步服务，基于 **Flink CDC** 实现 MySQL 到 Apache Doris 的实时数据同步。本项目不依赖 Spring Boot，是一个独立的 Flink Maven 工程。

## 技术栈

| 维度 | 选型 |
|------|------|
| 语言 / JDK | Java 17 |
| 计算框架 | Apache Flink 1.18.1 |
| CDC 连接器 | Flink CDC 3.0.1（mysql-cdc） |
| 目标端连接器 | Doris Flink Connector 25.1.0（flink-doris-connector-1.18） |
| Scala 二进制版本 | 2.12 |
| 构建 | Maven + maven-shade-plugin（打 uber jar） |

## 目录结构

```
trade-cdc
├── pom.xml                                  # 独立 Maven 构建配置
├── README.md
└── src/main
    ├── java/com/mtx/trade/cdc
    │   └── TradeCdcJob.java                 # Flink Job 启动类
    └── resources
        └── log4j2.xml                       # 日志配置
```

## 作业说明

`TradeCdcJob` 是一个骨架作业，演示 MySQL CDC → Doris 的完整链路配置：

1. 创建流执行环境与表环境，开启 checkpoint。
2. 通过 `mysql-cdc` 连接器注册源表 `orders_source`（读取 `trade_order.orders`）。
3. 通过 `doris` 连接器注册目标表 `orders_sink`（写入 Doris `trade_order.orders`）。
4. 执行 `INSERT INTO ... SELECT ...` 完成同步。

> 当前为全字段透传的基础工程，未实现具体业务逻辑，后续可在 SELECT 中扩展字段加工与转换规则。

## 配置示例

### MySQL CDC 源表关键选项

| 选项 | 示例值 | 说明 |
|------|--------|------|
| `connector` | mysql-cdc | 使用 Flink CDC MySQL 连接器 |
| `hostname` / `port` | localhost / 3306 | MySQL 地址 |
| `username` / `password` | root / root | 账号 |
| `database-name` | trade_order | 源库 |
| `table-name` | orders | 源表（支持正则） |
| `server-time-zone` | Asia/Shanghai | 时区 |
| `scan.startup.mode` | initial | 启动位点（initial / latest-offset） |

### Doris 目标表关键选项

| 选项 | 示例值 | 说明 |
|------|--------|------|
| `connector` | doris | Doris Flink 连接器 |
| `fenodes` | localhost:8030 | Doris FE HTTP 地址 |
| `table.identifier` | trade_order.orders | 库.表 |
| `username` / `password` | root / (空) | Doris 账号 |
| `sink.label-prefix` | trade_cdc_orders | Stream Load 标签前缀（幂等去重） |
| `sink.properties.format` | json | 写入格式 |

## 快速开始

### 前置依赖

- JDK 17
- Maven 3.8+
- Flink 1.18.x 集群（standalone 或 on yarn/k8s）
- MySQL 8（需开启 binlog，`trade_order.orders` 表）
- Apache Doris（FE 8030 可达）

### 构建打包

```bash
mvn clean package -DskipTests
# 产物：target/trade-cdc-1.0.0-SNAPSHOT.jar（uber jar，含 CDC 与 Doris 连接器）
```

### 提交作业

```bash
# 提交到 Flink 集群
flink run -c com.mtx.trade.cdc.TradeCdcJob target/trade-cdc-1.0.0-SNAPSHOT.jar

# 或本地直接运行（便于调试）
java -cp target/trade-cdc-1.0.0-SNAPSHOT.jar com.mtx.trade.cdc.TradeCdcJob
```

> 注意：Flink 核心依赖以 `provided` 作用域声明，提交到集群时由 Flink 运行时提供；本地直接运行需将 Flink 的 lib 加入 classpath。

## 配置改造建议

- 真实环境请将连接信息通过程序参数或配置文件注入，避免硬编码。
- 多表同步可扩展为多个 source/sink 对，或使用 Flink CDC Pipeline（YAML）进行整库同步。
- 生产环境需调整 checkpoint 间隔、并行度与 Doris sink 的 `sink.batch.size`、`sink.max-retries` 等参数。
