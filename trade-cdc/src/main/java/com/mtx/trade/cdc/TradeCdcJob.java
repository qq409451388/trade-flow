package com.mtx.trade.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 交易 CDC 同步作业骨架。
 *
 * <p>演示 MySQL（通过 Flink CDC）实时同步到 Apache Doris 的配置示例。
 * 当前为可编译的基础工程，未实现具体业务逻辑，后续按需扩展表结构与转换规则。</p>
 */
public class TradeCdcJob {

    public static void main(String[] args) throws Exception {
        // 1. 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60_000L);

        // 2. 表环境（流模式）
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 3. 创建 MySQL CDC 源表（配置示例）
        tEnv.executeSql(
                "CREATE TABLE orders_source (\n" +
                        "  id BIGINT,\n" +
                        "  order_no STRING,\n" +
                        "  amount DECIMAL(18, 2),\n" +
                        "  status STRING,\n" +
                        "  create_time TIMESTAMP(3),\n" +
                        "  update_time TIMESTAMP(3),\n" +
                        "  PRIMARY KEY (id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'mysql-cdc',\n" +
                        "  'hostname' = 'localhost',\n" +
                        "  'port' = '3306',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'root',\n" +
                        "  'database-name' = 'trade_order',\n" +
                        "  'table-name' = 'orders',\n" +
                        "  'server-time-zone' = 'Asia/Shanghai',\n" +
                        "  'scan.startup.mode' = 'initial'\n" +
                        ")"
        );

        // 4. 创建 Doris 目标表（配置示例）
        tEnv.executeSql(
                "CREATE TABLE orders_sink (\n" +
                        "  id BIGINT,\n" +
                        "  order_no STRING,\n" +
                        "  amount DECIMAL(18, 2),\n" +
                        "  status STRING,\n" +
                        "  create_time TIMESTAMP(3),\n" +
                        "  update_time TIMESTAMP(3)\n" +
                        ") WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = 'localhost:8030',\n" +
                        "  'table.identifier' = 'trade_order.orders',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '',\n" +
                        "  'sink.label-prefix' = 'trade_cdc_orders',\n" +
                        "  'sink.properties.format' = 'json',\n" +
                        "  'sink.properties.strip_outer_array' = 'true'\n" +
                        ")"
        );

        // 5. 执行同步（示例：全字段透传，后续可在此处加工）
        TableResult result = tEnv.executeSql(
                "INSERT INTO orders_sink\n" +
                        "SELECT id, order_no, amount, status, create_time, update_time\n" +
                        "FROM orders_source"
        );

        // 流式作业会持续运行，await 会阻塞直到作业取消
        result.await();
    }
}
