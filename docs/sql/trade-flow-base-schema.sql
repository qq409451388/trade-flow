-- trade_flow 最终基础结构：Ingress 事实事件、投递控制、接入失败审计及 Storage 模板表。
-- 在目标 trade_flow 数据库中执行；Storage 100组物理表由 trade-storage-shards.sql 创建。

CREATE TABLE `trade_storage` (
  `id` BIGINT NOT NULL COMMENT 'storage领域雪花ID，与payload_sha256共同定位Storage',
  `source_system` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '来源系统：0未知；1富友',
  `content_type` TINYINT UNSIGNED NOT NULL COMMENT '内容类型：1订单；2支付',
  `payload_sha256` BINARY(32) NOT NULL COMMENT '原始请求体字节SHA-256，同时作为分片键',
  `payload_length` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '原始请求体字节长度',
  `content_storage_type` TINYINT UNSIGNED NOT NULL DEFAULT 1 COMMENT '存储类型：1 BLOB；2本地归档；3 OSS归档',
  `content_ref` VARCHAR(512) NOT NULL DEFAULT '' COMMENT '归档文件相对路径或对象Key；BLOB类型为空',
  `content_offset` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '内容在归档文件内的字节偏移量；BLOB类型为0',
  `content_length` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '归档文件内存储内容长度；未归档时为0',
  `received_time` DATETIME(3) NOT NULL COMMENT '原始数据接收时间',
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '记录创建时间',
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '记录更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_source_sha256` (`source_system`, `payload_sha256`),
  KEY `idx_received_time` (`received_time`),
  KEY `idx_archive_scan` (`content_storage_type`, `received_time`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='原始数据元数据模板表，按SHA-256模100分表';

CREATE TABLE `trade_storage_blob` (
  `id` BIGINT NOT NULL COMMENT '与trade_storage.id严格相等',
  `payload_sha256` BINARY(32) NOT NULL COMMENT '与trade_storage.payload_sha256严格相等，同时作为分片键',
  `content` MEDIUMBLOB NOT NULL COMMENT '原始请求体字节',
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '记录创建时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='原始内容热存储模板表，按SHA-256模100分表';

CREATE TABLE `trade_order_event` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `source_system` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '来源系统：0未知；1富友',
  `third_event_key` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '第三方事件唯一键',
  `message_version` BIGINT UNSIGNED NOT NULL COMMENT '第三方消息版本，来源适配器转换为非负整数',
  `raw_id` BIGINT NOT NULL COMMENT '关联trade_storage.id',
  `payload_sha256` BINARY(32) NOT NULL COMMENT '原始报文SHA-256，与raw_id共同定位Storage',
  `acked` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Pipeline接管状态：0未ACK；1已ACK',
  `acked_time` DATETIME(3) NULL COMMENT 'Pipeline ACK时间',
  `received_time` DATETIME(3) NOT NULL COMMENT '事件接收时间',
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_raw_id` (`raw_id`),
  UNIQUE KEY `uk_event_version` (`source_system`, `third_event_key`, `message_version`),
  KEY `idx_acked_create_id` (`acked`, `create_time`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='订单接入事实事件，保存90天';

CREATE TABLE `trade_payment_event` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `source_system` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '来源系统：0未知；1富友',
  `third_event_key` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '第三方支付事件唯一键',
  `message_version` BIGINT UNSIGNED NOT NULL COMMENT '第三方消息版本，来源适配器转换为非负整数',
  `raw_id` BIGINT NOT NULL COMMENT '关联trade_storage.id',
  `payload_sha256` BINARY(32) NOT NULL COMMENT '原始报文SHA-256，与raw_id共同定位Storage',
  `acked` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Pipeline接管状态：0未ACK；1已ACK',
  `acked_time` DATETIME(3) NULL COMMENT 'Pipeline ACK时间',
  `received_time` DATETIME(3) NOT NULL COMMENT '事件接收时间',
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_raw_id` (`raw_id`),
  UNIQUE KEY `uk_event_version` (`source_system`, `third_event_key`, `message_version`),
  KEY `idx_acked_create_id` (`acked`, `create_time`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='支付接入事实事件，保存90天';

CREATE TABLE `trade_event_ingest_failure_log` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `request_id` CHAR(32) NOT NULL COMMENT '单次接入请求排障ID，与响应及服务端日志一致',
  `source_system` TINYINT UNSIGNED NOT NULL COMMENT '来源系统：0未知；1富友',
  `content_type` TINYINT UNSIGNED NOT NULL COMMENT '内容类型：1订单；2支付',
  `raw_id` BIGINT NULL COMMENT 'Storage成功时关联trade_storage.id；前置失败时为空',
  `payload_sha256` BINARY(32) NOT NULL COMMENT '原始内容SHA-256；Storage成功后与raw_id共同定位内容',
  `failure_stage` VARCHAR(32) NOT NULL COMMENT '失败阶段：SIGNATURE_VERIFY/STORAGE_PERSIST/EVENT_FIELD_PARSE/EVENT_PERSIST',
  `error_code` INT NOT NULL COMMENT '标准错误码',
  `exception_type` VARCHAR(255) NOT NULL COMMENT '最外层异常完整类名',
  `failure_reason` VARCHAR(1024) NOT NULL COMMENT '最多8层异常链摘要',
  `third_event_key` VARCHAR(128) NULL COMMENT '已成功提取时记录第三方事件键',
  `message_version` BIGINT UNSIGNED NULL COMMENT '已成功提取时记录第三方消息版本',
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '失败发生时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_request_id` (`request_id`),
  KEY `idx_storage` (`raw_id`, `payload_sha256`),
  KEY `idx_payload_time` (`payload_sha256`, `create_time`),
  KEY `idx_type_stage_time` (`content_type`, `failure_stage`, `create_time`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='第三方事件接入全阶段失败审计';

CREATE TABLE `trade_event_delivery_control` (
  `content_type` TINYINT UNSIGNED NOT NULL COMMENT '事件通道：1订单；2支付',
  `circuit_status` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Redis发布状态：0 CLOSED；1 OPEN',
  `failure_window_start` DATETIME(3) NULL COMMENT '当前失败统计窗口起点',
  `failure_count` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当前窗口Redis发布失败数',
  `opened_time` DATETIME(3) NULL COMMENT '最近进入OPEN时间',
  `next_health_check_time` DATETIME(3) NULL COMMENT '下次允许探活时间',
  `health_success_count` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'OPEN状态连续健康次数',
  `last_failure_time` DATETIME(3) NULL COMMENT '最近失败时间',
  `last_failure_reason` VARCHAR(1024) NULL COMMENT '最近失败原因',
  `recovery_owner` VARCHAR(64) NULL COMMENT '当前探活或积压恢复实例',
  `recovery_lease_until` DATETIME(3) NULL COMMENT '恢复任务租约截止时间',
  `version` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '状态变更版本',
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`content_type`),
  KEY `idx_status_health_time` (`circuit_status`, `next_health_check_time`),
  KEY `idx_recovery_lease` (`recovery_lease_until`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Ingress Redis Stream投递熔断持久化状态';

INSERT INTO `trade_event_delivery_control`
  (`content_type`, `circuit_status`, `failure_count`, `health_success_count`, `version`)
VALUES
  (1, 0, 0, 0, 0),
  (2, 0, 0, 0, 0);
