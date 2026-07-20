# trade-analytics

交易分析服务 - 提供交易数据分析能力。

## 技术栈

| 维度 | 选型 |
|------|------|
| 语言 / JDK | Java 17 |
| 框架 | Spring Boot 3.3.5 |
| Web | Spring Web |
| 参数校验 | Spring Validation |
| ORM | MyBatis-Plus 3.5.9 |
| 数据库 | MySQL 8 |
| 缓存 | Spring Data Redis (Lettuce) |
| 监控 | Spring Boot Actuator |
| 简化代码 | Lombok |

## 目录结构

```
trade-analytics
├── pom.xml                      # 独立 Maven 构建配置
├── Dockerfile                   # 容器镜像构建
├── README.md
└── src
    ├── main
    │   ├── java/com/mtx/trade/analytics
    │   │   ├── TradeAnalyticsApplication.java   # 启动类
    │   │   └── controller/HealthController.java  # 健康检查
    │   └── resources
    │       ├── application.yml        # 通用配置
    │       └── application-dev.yml    # 开发环境配置
    └── test
        └── java/com/mtx/trade/analytics
            └── TradeAnalyticsApplicationTests.java
```

## 快速开始

### 前置依赖

- JDK 17
- Maven 3.8+
- MySQL 8（需创建数据库 `trade_analytics`）
- Redis 7

### 本地运行

```bash
# 1. 准备数据库
mysql -uroot -proot -e "CREATE DATABASE IF NOT EXISTS trade_analytics DEFAULT CHARSET utf8mb4;"

# 2. 编译打包
mvn clean package -DskipTests

# 3. 启动
java -jar target/trade-analytics-1.0.0-SNAPSHOT.jar
```

启动后访问健康检查：
- 轻量探活：`GET http://localhost:8084/trade-analytics/ping`
- Actuator：`GET http://localhost:8084/trade-analytics/actuator/health`

### Docker 运行

```bash
mvn clean package -DskipTests
docker build -t trade-analytics:1.0.0 .
docker run -d -p 8084:8084 --name trade-analytics trade-analytics:1.0.0
```

## 配置说明

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `server.port` | 8084 | 服务端口 |
| `server.servlet.context-path` | /trade-analytics | 上下文路径 |
| `spring.datasource.url` | jdbc:mysql://localhost:3306/trade_analytics | MySQL 连接 |
| `spring.data.redis.host` | localhost | Redis 地址 |

业务配置按需在 `application-dev.yml` 中扩展，生产环境请新建 `application-prod.yml` 并通过 `--spring.profiles.active=prod` 激活。
