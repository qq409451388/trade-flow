-- Pipeline 耗尽事件自动拉取租约。
-- 已有环境执行本文件；全新环境直接执行 trade-pipeline-base-schema.sql。

CREATE TABLE IF NOT EXISTS `pipeline_event_pull_control` (
    `content_type` TINYINT UNSIGNED NOT NULL COMMENT '事件类型：1订单；2支付',
    `lease_owner` VARCHAR(64) NULL COMMENT '当前主动拉取实例',
    `lease_until` DATETIME(3) NULL COMMENT '主动拉取租约截止时间',
    `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`content_type`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pipeline耗尽事件主动拉取租约';

INSERT INTO `pipeline_event_pull_control` (`content_type`)
VALUES (1), (2)
ON DUPLICATE KEY UPDATE `content_type` = VALUES(`content_type`);
