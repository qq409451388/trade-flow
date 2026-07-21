-- Event 消息版本改造。
-- 执行前提：2026-07-21 检查时 trade_order_event / trade_payment_event 均为空表。
-- 如果其他环境已有数据，应先从原始报文回填 message_version，再创建唯一键。

ALTER TABLE `trade_order_event`
    ADD COLUMN `message_version` BIGINT UNSIGNED NOT NULL COMMENT '第三方消息版本，来源适配器转换为非负整数'
        AFTER `third_event_key`,
    DROP INDEX `uk_event_idempotent`,
    ADD UNIQUE KEY `uk_event_version` (`source_system`, `third_event_key`, `message_version`);

ALTER TABLE `trade_payment_event`
    ADD COLUMN `message_version` BIGINT UNSIGNED NOT NULL COMMENT '第三方消息版本，来源适配器转换为非负整数'
        AFTER `event_key`,
    DROP INDEX `uk_event_idempotent`,
    ADD UNIQUE KEY `uk_event_version` (`source_system`, `event_key`, `message_version`);
