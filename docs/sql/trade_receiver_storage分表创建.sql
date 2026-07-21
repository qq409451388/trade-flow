-- Storage 全新环境初始化脚本。
-- 分片规则：unsigned(payload_sha256) % 100；storageId 仍为雪花 ID。

CREATE TABLE IF NOT EXISTS `trade_storage` (
    `id` BIGINT NOT NULL COMMENT 'storage领域雪花ID，与payload_sha256共同定位Storage',
    `source_system` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '来源系统：0未知；1富友',
    `content_type` TINYINT UNSIGNED NOT NULL COMMENT '内容类型：1订单；2支付',
    `payload_sha256` BINARY(32) NOT NULL COMMENT '原始请求体字节SHA-256，同时作为分片键',
    `payload_length` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '原始请求体字节长度',
    `content_storage_type` TINYINT UNSIGNED NOT NULL DEFAULT 1 COMMENT '存储类型：1 BLOB；2本地归档；3 OSS归档',
    `content_ref` VARCHAR(512) NOT NULL DEFAULT '' COMMENT '归档文件相对路径或对象Key；BLOB类型为空',
    `content_offset` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '内容在归档文件内的字节偏移量；BLOB类型为0',
    `content_length` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '实际存储内容长度',
    `received_time` DATETIME(3) NOT NULL COMMENT '原始数据接收时间',
    `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '记录创建时间',
    `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '记录更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_source_sha256` (`source_system`, `payload_sha256`),
    KEY `idx_received_time` (`received_time`),
    KEY `idx_archive_scan` (`content_storage_type`, `received_time`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='原始数据元数据模板表，按SHA-256模100分表';

CREATE TABLE IF NOT EXISTS `trade_storage_blob` (
    `id` BIGINT NOT NULL COMMENT '与trade_storage.id严格相等',
    `payload_sha256` BINARY(32) NOT NULL COMMENT '与trade_storage.payload_sha256严格相等，同时作为分片键',
    `content` MEDIUMBLOB NOT NULL COMMENT '原始请求体字节',
    `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '记录创建时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='原始内容热存储模板表，按SHA-256模100分表';

DELIMITER $$

DROP PROCEDURE IF EXISTS `create_trade_storage_shards`$$

CREATE PROCEDURE `create_trade_storage_shards`()
BEGIN
    DECLARE shard_no INT DEFAULT 0;
    DECLARE shard_suffix CHAR(2);

    WHILE shard_no < 100 DO
        SET shard_suffix = LPAD(shard_no, 2, '0');

        SET @storage_sql = CONCAT(
            'CREATE TABLE IF NOT EXISTS `trade_storage_', shard_suffix,
            '` LIKE `trade_storage`'
        );
        PREPARE storage_stmt FROM @storage_sql;
        EXECUTE storage_stmt;
        DEALLOCATE PREPARE storage_stmt;

        SET @blob_sql = CONCAT(
            'CREATE TABLE IF NOT EXISTS `trade_storage_blob_', shard_suffix,
            '` LIKE `trade_storage_blob`'
        );
        PREPARE blob_stmt FROM @blob_sql;
        EXECUTE blob_stmt;
        DEALLOCATE PREPARE blob_stmt;

        SET shard_no = shard_no + 1;
    END WHILE;
END$$

DELIMITER ;

CALL `create_trade_storage_shards`();
DROP PROCEDURE `create_trade_storage_shards`;
