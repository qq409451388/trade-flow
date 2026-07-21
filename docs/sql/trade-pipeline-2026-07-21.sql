-- Pipeline 订单分表初始化（项目尚未投产，执行前应再次确认 4 张旧基础表为空）。
-- 逻辑表结构以 docs/sql/pipeline.sql 为准；本文件创建 2026、2027 两年的物理表。

ALTER TABLE `oms_order`
    DROP INDEX `uk_mchnt_order`,
    DROP INDEX `idx_order_no`,
    DROP COLUMN `finish_date`,
    ADD UNIQUE KEY `uk_order_no` (`order_no`);

ALTER TABLE `oms_order_item_spec`
    DROP INDEX `idx_order_no`,
    DROP COLUMN `order_no`;

RENAME TABLE
    `oms_order` TO `oms_order_2026`,
    `oms_order_item` TO `oms_order_item_2026_00`,
    `oms_order_item_spec` TO `oms_order_item_spec_2026`,
    `oms_order_package_item` TO `oms_order_package_item_2026`;

CREATE TABLE `oms_order_item_2026_01` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_02` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_03` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_04` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_05` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_06` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_07` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_08` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_09` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_10` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_11` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_12` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_13` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_14` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2026_15` LIKE `oms_order_item_2026_00`;

CREATE TABLE `oms_order_2027` LIKE `oms_order_2026`;
CREATE TABLE `oms_order_item_spec_2027` LIKE `oms_order_item_spec_2026`;
CREATE TABLE `oms_order_package_item_2027` LIKE `oms_order_package_item_2026`;
CREATE TABLE `oms_order_item_2027_00` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_01` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_02` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_03` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_04` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_05` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_06` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_07` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_08` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_09` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_10` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_11` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_12` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_13` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_14` LIKE `oms_order_item_2026_00`;
CREATE TABLE `oms_order_item_2027_15` LIKE `oms_order_item_2026_00`;

CREATE TABLE `pipeline_order_event_log` (
    `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `event_id` BIGINT NULL COMMENT 'Ingress订单event ID，消息无法解析时为空',
    `stream_record_id` VARCHAR(64) NULL COMMENT 'Redis Stream record ID，主动拉取时为空',
    `trigger_type` TINYINT UNSIGNED NOT NULL COMMENT '触发方式：1新消息；2 PEL接管；3主动拉取',
    `storage_id` BIGINT NULL COMMENT '关联trade_storage.id',
    `payload_sha256` BINARY(32) NULL COMMENT 'Storage SHA-256',
    `event_key` VARCHAR(128) NULL COMMENT '第三方事件键',
    `message_version` BIGINT UNSIGNED NULL COMMENT '第三方消息版本',
    `process_status` TINYINT UNSIGNED NOT NULL COMMENT '结果：1已应用；2重复或旧版本；3失败',
    `failure_stage` VARCHAR(32) NULL COMMENT '失败阶段',
    `error_code` INT NULL COMMENT '失败错误码',
    `failure_reason` VARCHAR(1024) NULL COMMENT '失败原因摘要',
    `started_time` DATETIME(3) NOT NULL COMMENT '开始处理时间',
    `finished_time` DATETIME(3) NOT NULL COMMENT '结束处理时间',
    `duration_ms` BIGINT UNSIGNED NOT NULL COMMENT '处理耗时毫秒',
    `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    KEY `idx_event_time` (`event_id`, `create_time`),
    KEY `idx_status_time` (`process_status`, `create_time`),
    KEY `idx_storage` (`storage_id`, `payload_sha256`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pipeline订单事件每次实际处理审计流水';
