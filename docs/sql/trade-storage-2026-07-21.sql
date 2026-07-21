-- Storage 从 id % 100 切换为 unsigned(payload_sha256) % 100。
-- 本脚本只适用于当前100组物理分表均为空的环境；发现数据会 SIGNAL 并终止。

DELIMITER $$

DROP PROCEDURE IF EXISTS `assert_and_drop_empty_storage_shards`$$

CREATE PROCEDURE `assert_and_drop_empty_storage_shards`()
BEGIN
    DECLARE shard_no INT DEFAULT 0;
    DECLARE shard_suffix CHAR(2);

    WHILE shard_no < 100 DO
        SET shard_suffix = LPAD(shard_no, 2, '0');

        SET @row_count = 0;
        SET @count_sql = CONCAT(
            'SELECT COUNT(*) INTO @row_count FROM `trade_storage_', shard_suffix, '`'
        );
        PREPARE count_stmt FROM @count_sql;
        EXECUTE count_stmt;
        DEALLOCATE PREPARE count_stmt;
        IF @row_count > 0 THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'Storage metadata 分片存在数据，禁止执行空表重建脚本';
        END IF;

        SET @row_count = 0;
        SET @count_sql = CONCAT(
            'SELECT COUNT(*) INTO @row_count FROM `trade_storage_blob_', shard_suffix, '`'
        );
        PREPARE count_stmt FROM @count_sql;
        EXECUTE count_stmt;
        DEALLOCATE PREPARE count_stmt;
        IF @row_count > 0 THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'Storage BLOB 分片存在数据，禁止执行空表重建脚本';
        END IF;

        SET shard_no = shard_no + 1;
    END WHILE;

    -- 全部100组都验证为空后才开始删除，避免中途 SIGNAL 造成部分分表已被删除。
    SET shard_no = 0;
    WHILE shard_no < 100 DO
        SET shard_suffix = LPAD(shard_no, 2, '0');
        SET @drop_sql = CONCAT(
            'DROP TABLE `trade_storage_', shard_suffix, '`, ',
            '`trade_storage_blob_', shard_suffix, '`'
        );
        PREPARE drop_stmt FROM @drop_sql;
        EXECUTE drop_stmt;
        DEALLOCATE PREPARE drop_stmt;
        SET shard_no = shard_no + 1;
    END WHILE;
END$$

DELIMITER ;

CALL `assert_and_drop_empty_storage_shards`();
DROP PROCEDURE `assert_and_drop_empty_storage_shards`;

ALTER TABLE `trade_storage`
    MODIFY COLUMN `id` BIGINT NOT NULL COMMENT 'storage领域雪花ID，与payload_sha256共同定位Storage',
    MODIFY COLUMN `payload_sha256` BINARY(32) NOT NULL COMMENT '原始请求体字节SHA-256，同时作为分片键',
    DROP INDEX `idx_payload_sha256`,
    ADD UNIQUE KEY `uk_source_sha256` (`source_system`, `payload_sha256`),
    COMMENT = '原始数据元数据模板表，按SHA-256模100分表';

ALTER TABLE `trade_storage_blob`
    MODIFY COLUMN `id` BIGINT NOT NULL COMMENT '与trade_storage.id严格相等',
    ADD COLUMN `payload_sha256` BINARY(32) NOT NULL
        COMMENT '与trade_storage.payload_sha256严格相等，同时作为分片键' AFTER `id`,
    COMMENT = '原始内容热存储模板表，按SHA-256模100分表';

DELIMITER $$

DROP PROCEDURE IF EXISTS `create_trade_storage_shards`$$

CREATE PROCEDURE `create_trade_storage_shards`()
BEGIN
    DECLARE shard_no INT DEFAULT 0;
    DECLARE shard_suffix CHAR(2);

    WHILE shard_no < 100 DO
        SET shard_suffix = LPAD(shard_no, 2, '0');

        SET @storage_sql = CONCAT(
            'CREATE TABLE `trade_storage_', shard_suffix, '` LIKE `trade_storage`'
        );
        PREPARE storage_stmt FROM @storage_sql;
        EXECUTE storage_stmt;
        DEALLOCATE PREPARE storage_stmt;

        SET @blob_sql = CONCAT(
            'CREATE TABLE `trade_storage_blob_', shard_suffix, '` LIKE `trade_storage_blob`'
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

-- 所有直接绑定 Storage 的执行流水同时保存 storage ID 与 SHA-256 路由键。
ALTER TABLE `trade_event_execution_log`
    ADD COLUMN `payload_sha256` BINARY(32) NULL
        COMMENT '原始报文SHA-256，与raw_id共同定位Storage' AFTER `raw_id`;

UPDATE `trade_event_execution_log` execution_log
LEFT JOIN `trade_order_event` order_event
    ON execution_log.event_type = 1 AND execution_log.event_id = order_event.id
LEFT JOIN `trade_payment_event` payment_event
    ON execution_log.event_type = 2 AND execution_log.event_id = payment_event.id
SET execution_log.payload_sha256 = CASE execution_log.event_type
    WHEN 1 THEN order_event.payload_sha256
    WHEN 2 THEN payment_event.payload_sha256
END
WHERE execution_log.payload_sha256 IS NULL;

DELIMITER $$

DROP PROCEDURE IF EXISTS `assert_execution_log_storage_sha`$$
CREATE PROCEDURE `assert_execution_log_storage_sha`()
BEGIN
    IF EXISTS (
        SELECT 1 FROM `trade_event_execution_log`
        WHERE `payload_sha256` IS NULL
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = '执行流水存在无法回填Storage SHA-256的数据';
    END IF;
END$$

DELIMITER ;

CALL `assert_execution_log_storage_sha`();
DROP PROCEDURE `assert_execution_log_storage_sha`;

ALTER TABLE `trade_event_execution_log`
    MODIFY COLUMN `payload_sha256` BINARY(32) NOT NULL
        COMMENT '原始报文SHA-256，与raw_id共同定位Storage',
    DROP INDEX `idx_raw_id`,
    ADD KEY `idx_storage_ref` (`raw_id`, `payload_sha256`);
