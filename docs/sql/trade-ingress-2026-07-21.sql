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

-- 自动补发最多执行指定次数，耗尽后等待人工处理。
ALTER TABLE `trade_order_event`
    DROP INDEX `idx_acked_create_time`,
    ADD COLUMN `auto_redelivery_count` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '15分钟后定时补发成功次数' AFTER `acked_time`,
    ADD COLUMN `last_redelivery_time` DATETIME(3) NULL
        COMMENT '最近一次定时补发成功时间' AFTER `auto_redelivery_count`,
    ADD KEY `idx_acked_redelivery_time`
        (`acked`, `auto_redelivery_count`, `create_time`, `id`);

-- Storage 已成功，但第三方关键字段解析或 event 持久化失败时保留审计记录。
CREATE TABLE `trade_event_ingest_failure_log` (
    `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `source_system` TINYINT UNSIGNED NOT NULL COMMENT '来源系统：0未知；1富友',
    `content_type` TINYINT UNSIGNED NOT NULL COMMENT '内容类型：1订单；2支付',
    `raw_id` BIGINT NOT NULL COMMENT '关联trade_storage.id',
    `payload_sha256` BINARY(32) NOT NULL COMMENT '原始内容SHA-256，与raw_id共同定位Storage',
    `failure_stage` VARCHAR(32) NOT NULL COMMENT '失败阶段：EVENT_FIELD_PARSE/EVENT_PERSIST',
    `error_code` INT NOT NULL COMMENT '标准错误码',
    `failure_reason` VARCHAR(1024) NOT NULL COMMENT '失败原因摘要',
    `third_event_key` VARCHAR(128) NULL COMMENT '已成功提取时记录第三方事件键',
    `message_version` BIGINT UNSIGNED NULL COMMENT '已成功提取时记录第三方消息版本',
    `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '失败发生时间',
    PRIMARY KEY (`id`),
    KEY `idx_storage` (`raw_id`, `payload_sha256`),
    KEY `idx_type_stage_time` (`content_type`, `failure_stage`, `create_time`),
    KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Storage成功但event未正常落库的接入失败审计';

CREATE TABLE `trade_event_delivery_control` (
    `content_type` TINYINT UNSIGNED NOT NULL COMMENT '事件通道：1订单；2支付',
    `circuit_status` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '熔断状态：0 CLOSED；1 OPEN；2 HALF_OPEN',
    `failure_window_start` DATETIME(3) NULL COMMENT '当前失败统计窗口起点',
    `failure_count` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当前窗口Redis发布失败数',
    `opened_time` DATETIME(3) NULL COMMENT '最近进入OPEN时间',
    `next_health_check_time` DATETIME(3) NULL COMMENT '下次允许探活时间',
    `health_success_count` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'OPEN状态连续健康次数',
    `last_failure_time` DATETIME(3) NULL COMMENT '最近失败时间',
    `last_failure_reason` VARCHAR(1024) NULL COMMENT '最近失败原因',
    `recovery_owner` VARCHAR(64) NULL COMMENT '当前探活或积压恢复实例',
    `recovery_lease_until` DATETIME(3) NULL COMMENT '恢复任务租约截止时间',
    `recovery_cursor_id` BIGINT NOT NULL DEFAULT 0 COMMENT '熔断积压恢复游标event ID',
    `recovery_cutoff_id` BIGINT NOT NULL DEFAULT 0 COMMENT '本轮熔断积压截止event ID',
    `version` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '状态变更版本',
    `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`content_type`),
    KEY `idx_status_health_time` (`circuit_status`, `next_health_check_time`),
    KEY `idx_recovery_lease` (`recovery_lease_until`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Ingress Redis Stream投递熔断持久化状态';

INSERT IGNORE INTO `trade_event_delivery_control`
    (`content_type`, `circuit_status`, `failure_count`, `health_success_count`,
     `recovery_cursor_id`, `recovery_cutoff_id`, `version`)
VALUES
    (1, 0, 0, 0, 0, 0, 0),
    (2, 0, 0, 0, 0, 0, 0);

ALTER TABLE `trade_payment_event`
    DROP INDEX `idx_acked_create_time`,
    ADD COLUMN `auto_redelivery_count` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '15分钟后定时补发成功次数' AFTER `acked_time`,
    ADD COLUMN `last_redelivery_time` DATETIME(3) NULL
        COMMENT '最近一次定时补发成功时间' AFTER `auto_redelivery_count`,
    ADD KEY `idx_acked_redelivery_time`
        (`acked`, `auto_redelivery_count`, `create_time`, `id`);
