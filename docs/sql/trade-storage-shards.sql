-- 从 trade-flow-base-schema.sql 中的 Storage 模板表创建100组物理分表。
-- 在目标 trade_flow 数据库中执行。

DELIMITER $$

DROP PROCEDURE IF EXISTS `create_trade_storage_shards`$$

CREATE PROCEDURE `create_trade_storage_shards`()
BEGIN
  DECLARE shard_no INT DEFAULT 0;
  DECLARE shard_suffix CHAR(2);

  WHILE shard_no < 100 DO
    SET shard_suffix = LPAD(shard_no, 2, '0');

    SET @storage_sql = CONCAT(
      'CREATE TABLE IF NOT EXISTS `trade_storage_', shard_suffix, '` LIKE `trade_storage`'
    );
    PREPARE storage_stmt FROM @storage_sql;
    EXECUTE storage_stmt;
    DEALLOCATE PREPARE storage_stmt;

    SET @blob_sql = CONCAT(
      'CREATE TABLE IF NOT EXISTS `trade_storage_blob_', shard_suffix, '` LIKE `trade_storage_blob`'
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
