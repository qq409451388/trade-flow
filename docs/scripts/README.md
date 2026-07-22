# 运维与测试脚本

此目录存放项目相关的单文件临时工具。

- `replay-fuiou-push.sh`：按时间范围流式读取线上富友推送日志，边读取边回放到本机 Ingress，并每 2 秒输出读取、请求中、成功及失败数量。运行时会输出临时调试目录，其中 `active/` 是正在发送的 JSON，`results/` 保存已完成请求的 HTTP 状态和响应体；失败或中断时保留该目录。
- `verify-replay-idempotency.sh`：按与回放相同的源时间范围重新只读提取幂等键和 SHA，分别校验订单或支付在 Ingress event、Pipeline 年度业务表中的缺失、重复、版本落后及原文冲突；默认等待 Pipeline 最多 120 秒完成消费。
- `reset-trade-databases.sh`：使用 `TRUNCATE TABLE` 清空本机 `trade_flow`、`trade_pipeline` 数据并恢复必要种子记录。

注意：

- 回放脚本包含临时线上数据库凭据，仅限受控环境使用。
- 幂等校验脚本同样包含临时源库凭据，但对源库仅执行带 `push_receive_time` 范围条件的 `SELECT push_body`；本机校验只创建会话级临时表，不修改业务表。
- 数据库还原脚本具有破坏性，只允许默认连接本机数据库，执行前必须显式确认。
- 运行数据库还原前应停止 Ingress 和 Pipeline，避免清理过程中产生新写入。
- 数据库还原脚本不清理 Redis；如需完整重放，应另行确认本机 Redis Stream 状态。
