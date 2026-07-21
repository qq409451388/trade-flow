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

-- 支付事件第三方事件键命名与订单事件保持一致。
ALTER TABLE `trade_payment_event`
    DROP INDEX `uk_event_version`,
    CHANGE COLUMN `event_key` `third_event_key` VARCHAR(128) NOT NULL DEFAULT ''
        COMMENT '第三方支付事件唯一键，由接入适配器生成',
    ADD UNIQUE KEY `uk_event_version` (`source_system`, `third_event_key`, `message_version`);

-- event 表只记录 Ingress 到 Pipeline 的接管 ACK，不记录 Pipeline 执行结果。
ALTER TABLE `trade_order_event`
    DROP INDEX `idx_status_time`,
    ADD COLUMN `acked` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Pipeline接管状态：0未ACK；1已ACK'
        AFTER `payload_sha256`,
    ADD COLUMN `acked_time` DATETIME(3) NULL COMMENT 'Pipeline ACK时间'
        AFTER `acked`,
    DROP COLUMN `event_status`,
    DROP COLUMN `last_execution_id`,
    DROP COLUMN `success_time`,
    ADD KEY `idx_acked_create_time` (`acked`, `create_time`, `id`);

ALTER TABLE `trade_payment_event`
    DROP INDEX `idx_status_time`,
    ADD COLUMN `acked` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Pipeline接管状态：0未ACK；1已ACK'
        AFTER `payload_sha256`,
    ADD COLUMN `acked_time` DATETIME(3) NULL COMMENT 'Pipeline ACK时间'
        AFTER `acked`,
    DROP COLUMN `event_status`,
    DROP COLUMN `last_execution_id`,
    DROP COLUMN `success_time`,
    ADD KEY `idx_acked_create_time` (`acked`, `create_time`, `id`);
