-- ============================================================
-- Fake 测试库初始化脚本
-- 创建 trade_flow_fake 和 trade_pipeline_fake 数据库及全部表结构
-- 执行方式：mysql -u root < trade-fake-databases.sql
-- ============================================================

-- ============================================================
-- 1. 创建数据库
-- ============================================================
CREATE DATABASE IF NOT EXISTS `trade_flow_fake`
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS `trade_pipeline_fake`
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- ============================================================
-- 2. trade_flow_fake 库：Storage + Ingress 基础设施
-- ============================================================
USE `trade_flow_fake`;

-- 原始数据元数据模板表（100张分表之模板，实际使用 _00 ~ _99）
CREATE TABLE `trade_storage` (
  `id` BIGINT NOT NULL COMMENT 'storage领域雪花ID',
  `source_system` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '来源系统：0未知；1富友',
  `content_type` TINYINT UNSIGNED NOT NULL COMMENT '内容类型：1订单；2支付',
  `payload_sha256` BINARY(32) NOT NULL COMMENT '原始请求体字节SHA-256',
  `payload_length` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '原始请求体字节长度',
  `content_storage_type` TINYINT UNSIGNED NOT NULL DEFAULT 1 COMMENT '存储类型：1 BLOB',
  `content_ref` VARCHAR(512) NOT NULL DEFAULT '' COMMENT '归档文件相对路径',
  `content_offset` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '归档偏移量',
  `content_length` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '归档内容长度',
  `received_time` DATETIME(3) NOT NULL COMMENT '原始数据接收时间',
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_source_sha256` (`source_system`, `payload_sha256`),
  KEY `idx_received_time` (`received_time`),
  KEY `idx_archive_scan` (`content_storage_type`, `received_time`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='原始数据元数据模板表';

CREATE TABLE `trade_storage_blob` (
  `id` BIGINT NOT NULL COMMENT '与trade_storage.id严格相等',
  `payload_sha256` BINARY(32) NOT NULL COMMENT '与trade_storage.payload_sha256严格相等',
  `content` MEDIUMBLOB NOT NULL COMMENT '原始请求体字节',
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='原始内容热存储模板表';

-- 订单接入事实事件
CREATE TABLE `trade_order_event` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `source_system` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '来源系统',
  `third_event_key` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '第三方事件唯一键',
  `message_version` BIGINT UNSIGNED NOT NULL COMMENT '第三方消息版本',
  `raw_id` BIGINT NOT NULL COMMENT '关联trade_storage.id',
  `payload_sha256` BINARY(32) NOT NULL COMMENT '原始报文SHA-256',
  `acked` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Pipeline接管状态',
  `acked_time` DATETIME(3) NULL,
  `received_time` DATETIME(3) NOT NULL COMMENT '事件接收时间',
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_raw_id` (`raw_id`),
  UNIQUE KEY `uk_event_version` (`source_system`, `third_event_key`, `message_version`),
  KEY `idx_acked_create_id` (`acked`, `create_time`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='订单接入事实事件';

-- 支付接入事实事件
CREATE TABLE `trade_payment_event` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `source_system` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `third_event_key` VARCHAR(128) NOT NULL DEFAULT '',
  `message_version` BIGINT UNSIGNED NOT NULL,
  `raw_id` BIGINT NOT NULL COMMENT '关联trade_storage.id',
  `payload_sha256` BINARY(32) NOT NULL COMMENT '原始报文SHA-256',
  `acked` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `acked_time` DATETIME(3) NULL,
  `received_time` DATETIME(3) NOT NULL,
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_raw_id` (`raw_id`),
  UNIQUE KEY `uk_event_version` (`source_system`, `third_event_key`, `message_version`),
  KEY `idx_acked_create_id` (`acked`, `create_time`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='支付接入事实事件';

-- 接入失败审计
CREATE TABLE `trade_event_ingest_failure_log` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `request_id` CHAR(32) NOT NULL,
  `source_system` TINYINT UNSIGNED NOT NULL,
  `content_type` TINYINT UNSIGNED NOT NULL,
  `raw_id` BIGINT NULL,
  `payload_sha256` BINARY(32) NOT NULL,
  `failure_stage` VARCHAR(32) NOT NULL,
  `error_code` INT NOT NULL,
  `exception_type` VARCHAR(255) NOT NULL,
  `failure_reason` VARCHAR(1024) NOT NULL,
  `third_event_key` VARCHAR(128) NULL,
  `message_version` BIGINT UNSIGNED NULL,
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_request_id` (`request_id`),
  KEY `idx_storage` (`raw_id`, `payload_sha256`),
  KEY `idx_payload_time` (`payload_sha256`, `create_time`),
  KEY `idx_type_stage_time` (`content_type`, `failure_stage`, `create_time`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='第三方事件接入全阶段失败审计';

-- 投递熔断控制
CREATE TABLE `trade_event_delivery_control` (
  `content_type` TINYINT UNSIGNED NOT NULL COMMENT '1订单；2支付',
  `circuit_status` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `failure_window_start` DATETIME(3) NULL,
  `failure_count` INT UNSIGNED NOT NULL DEFAULT 0,
  `opened_time` DATETIME(3) NULL,
  `next_health_check_time` DATETIME(3) NULL,
  `health_success_count` INT UNSIGNED NOT NULL DEFAULT 0,
  `last_failure_time` DATETIME(3) NULL,
  `last_failure_reason` VARCHAR(1024) NULL,
  `recovery_owner` VARCHAR(64) NULL,
  `recovery_lease_until` DATETIME(3) NULL,
  `version` INT UNSIGNED NOT NULL DEFAULT 0,
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`content_type`),
  KEY `idx_status_health_time` (`circuit_status`, `next_health_check_time`),
  KEY `idx_recovery_lease` (`recovery_lease_until`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Ingress Redis Stream投递熔断持久化状态';

INSERT INTO `trade_event_delivery_control`
  (`content_type`, `circuit_status`, `failure_count`, `health_success_count`, `version`)
VALUES (1, 0, 0, 0, 0), (2, 0, 0, 0, 0);

-- ============================================================
-- 3. 创建 Storage 100 组分表 (trade_storage_00 ~ _99)
-- ============================================================
DELIMITER //
CREATE PROCEDURE create_trade_storage_shards()
BEGIN
  DECLARE i INT DEFAULT 0;
  WHILE i < 100 DO
    SET @sql = CONCAT(
      'CREATE TABLE `trade_storage_', LPAD(i, 2, '0'), '` LIKE `trade_storage`'
    );
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @sql = CONCAT(
      'CREATE TABLE `trade_storage_blob_', LPAD(i, 2, '0'), '` LIKE `trade_storage_blob`'
    );
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET i = i + 1;
  END WHILE;
END //
DELIMITER ;

CALL create_trade_storage_shards();
DROP PROCEDURE create_trade_storage_shards;

-- ============================================================
-- 4. trade_pipeline_fake 库：业务表
-- ============================================================
USE `trade_pipeline_fake`;

-- 订单模板表
CREATE TABLE `oms_order` (
  `id` BIGINT NOT NULL COMMENT '全局雪花ID',
  `storage_id` BIGINT NOT NULL COMMENT 'Storage 原文ID',
  `payload_sha256` BINARY(32) NOT NULL COMMENT 'Storage 原文SHA-256',
  `order_no` BIGINT NOT NULL COMMENT '富友订单号',
  `user_id` BIGINT DEFAULT NULL,
  `mchnt_cd` VARCHAR(32) NOT NULL DEFAULT '',
  `shop_id` BIGINT DEFAULT NULL,
  `express_id` BIGINT DEFAULT NULL,
  `tm_fuiou_id` VARCHAR(64) NOT NULL DEFAULT '',
  `term_name` VARCHAR(128) NOT NULL DEFAULT '',
  `order_type` VARCHAR(4) NOT NULL DEFAULT '' COMMENT '01小程序堂食；02外卖；03收银机支付',
  `channel_type` VARCHAR(4) NOT NULL DEFAULT '',
  `order_state` VARCHAR(4) NOT NULL DEFAULT '',
  `order_pay_state` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '1已支付；8部分退款；9已退款',
  `express_state` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `order_amt` BIGINT NOT NULL DEFAULT 0 COMMENT '商品打折前金额（分）',
  `order_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `pay_amt` BIGINT NOT NULL DEFAULT 0 COMMENT '实际支付金额（分）',
  `pay_amt_extra` BIGINT NOT NULL DEFAULT 0,
  `cash_received_amt` BIGINT NOT NULL DEFAULT 0,
  `refund_amt` BIGINT NOT NULL DEFAULT 0,
  `express_amt` BIGINT NOT NULL DEFAULT 0,
  `mchnt_express_cost` BIGINT NOT NULL DEFAULT 0,
  `lunch_box_fee` BIGINT NOT NULL DEFAULT 0,
  `invoice_amt` BIGINT NOT NULL DEFAULT 0,
  `third_mchnt_income` BIGINT NOT NULL DEFAULT 0,
  `order_create_time` DATETIME(3) NOT NULL COMMENT '年度分表依据',
  `pay_deadline_time` DATETIME(3) DEFAULT NULL,
  `pay_time` DATETIME(3) DEFAULT NULL,
  `cashier_confirm_time` DATETIME(3) DEFAULT NULL,
  `delivery_start_time` DATETIME(3) DEFAULT NULL,
  `comment_time` DATETIME(3) DEFAULT NULL,
  `finish_time` DATETIME(3) DEFAULT NULL,
  `source_update_time` DATETIME(3) DEFAULT NULL,
  `refund_time` DATETIME(3) DEFAULT NULL,
  `open_table_time` DATETIME(3) DEFAULT NULL,
  `meal_time` DATETIME(3) DEFAULT NULL,
  `estimated_finish_time` DATETIME(3) DEFAULT NULL,
  `reverse_time` DATETIME(3) DEFAULT NULL,
  `deliver_time_desc` VARCHAR(64) NOT NULL DEFAULT '',
  `pay_ssn` VARCHAR(64) NOT NULL DEFAULT '',
  `pay_type` VARCHAR(32) NOT NULL DEFAULT '',
  `pay_type_extra` VARCHAR(32) NOT NULL DEFAULT '',
  `third_order_no` VARCHAR(64) NOT NULL DEFAULT '',
  `app_open_id` VARCHAR(128) NOT NULL DEFAULT '',
  `out_user_id` VARCHAR(64) NOT NULL DEFAULT '',
  `meal_code` VARCHAR(32) NOT NULL DEFAULT '',
  `user_memo` VARCHAR(512) NOT NULL DEFAULT '',
  `order_cancel_reason` VARCHAR(512) NOT NULL DEFAULT '',
  `order_comment` VARCHAR(1024) NOT NULL DEFAULT '',
  `comment_state` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `comment_level` VARCHAR(4) NOT NULL DEFAULT '',
  `order_addr_id` BIGINT DEFAULT NULL,
  `phone` VARCHAR(20) NOT NULL DEFAULT '',
  `contact_mobile` VARCHAR(20) NOT NULL DEFAULT '',
  `express_company` VARCHAR(4) NOT NULL DEFAULT '' COMMENT '01达达；02自配送',
  `cashier_id` VARCHAR(32) NOT NULL DEFAULT '',
  `cashier_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `single_goods_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `full_order_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `cashier_discount` DECIMAL(7,2) DEFAULT NULL,
  `cashier_dis_id` BIGINT DEFAULT NULL,
  `cashier_dis_name` VARCHAR(64) NOT NULL DEFAULT '',
  `discount_type` VARCHAR(4) NOT NULL DEFAULT '',
  `discount_type_extra` VARCHAR(4) NOT NULL DEFAULT '',
  `coupon_id` BIGINT DEFAULT NULL,
  `coupon_real_id` BIGINT DEFAULT NULL,
  `special_coupon_id` VARCHAR(64) NOT NULL DEFAULT '',
  `special_mchnt_type` VARCHAR(4) NOT NULL DEFAULT '',
  `integral` BIGINT NOT NULL DEFAULT 0,
  `integral_deduction_amt` BIGINT NOT NULL DEFAULT 0,
  `coupon_amt` BIGINT NOT NULL DEFAULT 0,
  `full_minus_amt` BIGINT NOT NULL DEFAULT 0,
  `member_level_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `member_price_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `member_day_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `unionpay_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `times_card_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `package_price_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `group_pay_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `plat_hong_bao` BIGINT NOT NULL DEFAULT 0,
  `wipe_zero_amt` BIGINT NOT NULL DEFAULT 0,
  `not_in_discount_amt` BIGINT NOT NULL DEFAULT 0,
  `free_consume_amt` BIGINT NOT NULL DEFAULT 0,
  `fixed_price_amt` BIGINT NOT NULL DEFAULT 0,
  `group_pay_num` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `group_pay_num_extra` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `is_membership` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `is_meal_order` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `is_order_locked` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `has_reverse` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `is_pad_confirm` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `is_account_order` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `print_settle_status` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `third_basket_status` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `invoice_state` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `mqtt_send_state` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `meal_confirm_channel` VARCHAR(4) NOT NULL DEFAULT '',
  `table_fuiou_id` VARCHAR(64) NOT NULL DEFAULT '',
  `table_term_name` VARCHAR(128) NOT NULL DEFAULT '',
  `promoter_no` VARCHAR(32) NOT NULL DEFAULT '',
  `account_memo` VARCHAR(512) NOT NULL DEFAULT '',
  `member_phone` VARCHAR(20) NOT NULL DEFAULT '',
  `member_name` VARCHAR(64) NOT NULL DEFAULT '',
  `member_points` BIGINT NOT NULL DEFAULT 0,
  `user_balance` BIGINT NOT NULL DEFAULT 0,
  `guests_count` SMALLINT DEFAULT NULL,
  `order_version` SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_order_no` (`order_no`),
  KEY `idx_shop_create_time` (`mchnt_cd`, `shop_id`, `order_create_time`),
  KEY `idx_state_create_time` (`mchnt_cd`, `order_state`, `order_create_time`),
  KEY `idx_pay_ssn` (`pay_ssn`),
  KEY `idx_third_order_no` (`third_order_no`),
  KEY `idx_source_update_time` (`source_update_time`),
  KEY `idx_update_time` (`update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC
  COMMENT='富友订单当前快照基础表';

-- 订单商品明细模板表
CREATE TABLE `oms_order_item` (
  `id` BIGINT NOT NULL,
  `detail_no` BIGINT NOT NULL COMMENT '富友订单商品明细唯一编号',
  `order_no` BIGINT NOT NULL COMMENT 'Hash分片键',
  `shop_id` BIGINT NOT NULL,
  `mchnt_cd` VARCHAR(32) NOT NULL DEFAULT '',
  `goods_id` BIGINT NOT NULL DEFAULT 0,
  `goods_name` VARCHAR(128) NOT NULL DEFAULT '',
  `goods_unit` VARCHAR(32) NOT NULL DEFAULT '',
  `goods_bar_code` VARCHAR(64) NOT NULL DEFAULT '',
  `goods_base_price` BIGINT NOT NULL DEFAULT 0,
  `goods_dis_price` BIGINT NOT NULL DEFAULT 0,
  `goods_price` BIGINT NOT NULL DEFAULT 0 COMMENT '商品总价（分）',
  `goods_number` DECIMAL(14,3) NOT NULL DEFAULT 0.000,
  `refund_goods_number` DECIMAL(14,3) NOT NULL DEFAULT 0.000,
  `goods_pay_amt` BIGINT NOT NULL DEFAULT 0,
  `goods_real_pay_amt` BIGINT NOT NULL DEFAULT 0,
  `goods_total_refund_amt` BIGINT NOT NULL DEFAULT 0,
  `cashier_dis_price` BIGINT NOT NULL DEFAULT 0,
  `goods_cashier_discount` DECIMAL(5,2) NOT NULL DEFAULT 0.00,
  `dis_discount_reason` VARCHAR(64) NOT NULL DEFAULT '',
  `goods_dis_type` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `goods_member_price` BIGINT NOT NULL DEFAULT 0,
  `goods_member_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `goods_member_points` BIGINT NOT NULL DEFAULT 0,
  `promotion_id` BIGINT DEFAULT NULL,
  `goods_promotion_way` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `unionpay_coupon_id` VARCHAR(64) NOT NULL DEFAULT '',
  `avg_purchase_price` BIGINT NOT NULL DEFAULT 0,
  `goods_sales_commission` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `goods_employee_commission` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `spec_id_list` VARCHAR(128) NOT NULL DEFAULT '',
  `spec_desc_list` VARCHAR(128) NOT NULL DEFAULT '',
  `is_package_goods` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `in_pkg_goods_price` BIGINT NOT NULL DEFAULT 0,
  `goods_basket` VARCHAR(16) NOT NULL DEFAULT '',
  `goods_lunch_box_fee` BIGINT NOT NULL DEFAULT 0,
  `prepackaged_amt` BIGINT NOT NULL DEFAULT 0,
  `is_third_order` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `add_dish_channel` VARCHAR(4) NOT NULL DEFAULT '',
  `dish_state` TINYINT UNSIGNED NOT NULL DEFAULT 1,
  `dish_user_memo` VARCHAR(128) NOT NULL DEFAULT '',
  `dish_cashier_memo` VARCHAR(128) NOT NULL DEFAULT '',
  `dish_cancel_reason` VARCHAR(256) NOT NULL DEFAULT '',
  `dish_fuiou_id` VARCHAR(32) NOT NULL DEFAULT '',
  `dish_user_id` BIGINT DEFAULT NULL,
  `dish_cashier_id` VARCHAR(32) NOT NULL DEFAULT '',
  `dish_has_hurried` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `dish_has_finish` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `is_dish_confirm` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `is_opr_by_pad` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `is_weigh_goods` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `mark_print` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `dish_print_state` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `kitchen_print` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `print_serial_no` VARCHAR(64) NOT NULL DEFAULT '',
  `calc_model` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `item_create_time` DATETIME(3) NOT NULL,
  `dish_update_time` DATETIME(3) NOT NULL,
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_detail_no` (`detail_no`),
  KEY `idx_order_no` (`order_no`),
  KEY `idx_goods_id` (`goods_id`),
  KEY `idx_shop_item_time` (`mchnt_cd`, `shop_id`, `item_create_time`),
  KEY `idx_update_time` (`update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC
  COMMENT='订单商品明细基础表';

-- 订单商品规格明细模板表
CREATE TABLE `oms_order_item_spec` (
  `id` BIGINT NOT NULL,
  `order_spec_id` BIGINT NOT NULL COMMENT '富友订单规格明细唯一编号',
  `detail_no` BIGINT NOT NULL COMMENT '关联订单商品明细编号',
  `spec_id` BIGINT NOT NULL DEFAULT 0,
  `spec_name` VARCHAR(64) NOT NULL DEFAULT '',
  `spec_detail_id` BIGINT NOT NULL DEFAULT 0,
  `spec_detail_desc` VARCHAR(128) NOT NULL DEFAULT '',
  `detail_extra_price` BIGINT NOT NULL DEFAULT 0,
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_order_spec_id` (`order_spec_id`),
  KEY `idx_detail_no` (`detail_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='订单商品规格明细基础表';

-- 订单套餐子商品明细模板表
CREATE TABLE `oms_order_package_item` (
  `id` BIGINT NOT NULL,
  `package_detail_no` BIGINT NOT NULL COMMENT '富友套餐商品明细唯一编号',
  `relate_pkg_detail_no` BIGINT NOT NULL COMMENT '关联主商品明细detail_no',
  `order_no` BIGINT NOT NULL,
  `goods_id` BIGINT NOT NULL DEFAULT 0,
  `shop_id` BIGINT NOT NULL,
  `mchnt_cd` VARCHAR(32) NOT NULL DEFAULT '',
  `goods_name` VARCHAR(128) NOT NULL DEFAULT '',
  `goods_unit` VARCHAR(32) NOT NULL DEFAULT '',
  `goods_bar_code` VARCHAR(64) NOT NULL DEFAULT '',
  `goods_base_price` BIGINT NOT NULL DEFAULT 0,
  `goods_dis_price` BIGINT NOT NULL DEFAULT 0,
  `goods_member_price` BIGINT NOT NULL DEFAULT 0,
  `goods_member_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `goods_cashier_discount` DECIMAL(7,2) NOT NULL DEFAULT 0.00,
  `cashier_dis_price` BIGINT NOT NULL DEFAULT 0,
  `goods_price` BIGINT NOT NULL DEFAULT 0,
  `goods_pay_amt` BIGINT NOT NULL DEFAULT 0,
  `goods_number` DECIMAL(14,3) NOT NULL DEFAULT 0.000,
  `package_number` DECIMAL(14,3) NOT NULL DEFAULT 0.000,
  `pkg_goods_copies` DECIMAL(14,3) NOT NULL DEFAULT 0.000,
  `pkg_goods_extra_price` BIGINT NOT NULL DEFAULT 0,
  `avg_purchase_price` BIGINT NOT NULL DEFAULT 0,
  `goods_lunch_box_fee` BIGINT NOT NULL DEFAULT 0,
  `goods_member_points` BIGINT NOT NULL DEFAULT 0,
  `goods_index` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `mark_print` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `is_weigh_goods` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `spec_id_list` VARCHAR(64) NOT NULL DEFAULT '',
  `spec_desc_list` VARCHAR(128) NOT NULL DEFAULT '',
  `third_detail_no` VARCHAR(64) NOT NULL DEFAULT '',
  `package_dish_memo` VARCHAR(128) NOT NULL DEFAULT '',
  `item_create_time` DATETIME(3) NOT NULL,
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_package_detail_no` (`package_detail_no`),
  KEY `idx_relate_pkg_detail_no` (`relate_pkg_detail_no`),
  KEY `idx_order_no` (`order_no`),
  KEY `idx_goods_id` (`goods_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC
  COMMENT='订单套餐子商品明细基础表';

-- 支付流水模板表
CREATE TABLE `oms_payment` (
  `id` BIGINT UNSIGNED NOT NULL,
  `pay_ssn` VARCHAR(64) NOT NULL COMMENT '支付或退款流水号',
  `source_pay_ssn` VARCHAR(64) NOT NULL DEFAULT '',
  `order_no` BIGINT NOT NULL DEFAULT 0,
  `mchnt_cd` VARCHAR(32) NOT NULL,
  `shop_id` BIGINT NOT NULL DEFAULT 0,
  `shop_name` VARCHAR(64) NOT NULL DEFAULT '',
  `pay_time` DATETIME(3) NOT NULL COMMENT '年度分表依据',
  `refund_time` DATETIME(3) DEFAULT NULL,
  `pay_type` VARCHAR(16) NOT NULL DEFAULT '',
  `pay_name` VARCHAR(32) NOT NULL DEFAULT '',
  `pay_state` TINYINT UNSIGNED NOT NULL COMMENT '1支付成功，2退款成功',
  `pay_amt` BIGINT NOT NULL DEFAULT 0,
  `fee_amt` BIGINT NOT NULL DEFAULT 0,
  `refund_amt` BIGINT NOT NULL DEFAULT 0,
  `balance_dis_amt` BIGINT NOT NULL DEFAULT 0,
  `face_amt` BIGINT NOT NULL DEFAULT 0,
  `fy_settle` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `third_order_no` VARCHAR(64) NOT NULL DEFAULT '',
  `channel_trade_no` VARCHAR(64) NOT NULL DEFAULT '',
  `big_category` VARCHAR(4) NOT NULL DEFAULT '',
  `small_category` VARCHAR(4) NOT NULL DEFAULT '',
  `order_source` VARCHAR(4) NOT NULL DEFAULT '',
  `open_id` VARCHAR(64) NOT NULL DEFAULT '',
  `order_type` VARCHAR(4) NOT NULL DEFAULT '',
  `channel_type` VARCHAR(4) NOT NULL DEFAULT '',
  `member_name` VARCHAR(128) NOT NULL DEFAULT '',
  `phone` VARCHAR(20) NOT NULL DEFAULT '',
  `member_card_no` VARCHAR(64) NOT NULL DEFAULT '',
  `member_level` VARCHAR(4) NOT NULL DEFAULT '',
  `storage_id` BIGINT UNSIGNED NOT NULL,
  `payload_sha256` BINARY(32) NOT NULL,
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_pay_ssn` (`pay_ssn`),
  KEY `idx_source_pay_ssn` (`source_pay_ssn`),
  KEY `idx_order_no` (`order_no`),
  KEY `idx_third_order_no` (`third_order_no`),
  KEY `idx_channel_trade_no` (`channel_trade_no`),
  KEY `idx_pay_time` (`pay_time`),
  KEY `idx_mchnt_shop_time` (`mchnt_cd`, `shop_id`, `pay_time`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='支付及退款流水表';

-- 支付结算账户明细模板表
CREATE TABLE `oms_payment_account` (
  `id` BIGINT UNSIGNED NOT NULL,
  `payment_id` BIGINT UNSIGNED NOT NULL,
  `pay_ssn` VARCHAR(64) NOT NULL,
  `account_seq` SMALLINT UNSIGNED NOT NULL,
  `cust_acnt_tp` VARCHAR(4) NOT NULL DEFAULT '' COMMENT 'G对公，S对私',
  `mchnt_cd` VARCHAR(32) NOT NULL DEFAULT '',
  `shop_id` BIGINT NOT NULL DEFAULT 0,
  `out_acnt_nm` VARCHAR(128) NOT NULL DEFAULT '',
  `out_acnt_no` VARCHAR(128) NOT NULL DEFAULT '',
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_payment_account_seq` (`payment_id`, `account_seq`),
  KEY `idx_pay_ssn` (`pay_ssn`),
  KEY `idx_mchnt_shop` (`mchnt_cd`, `shop_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='支付结算账户明细表';

-- Pipeline审计日志
CREATE TABLE `pipeline_order_event_log` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `event_id` BIGINT NULL,
  `stream_record_id` VARCHAR(64) NULL,
  `trigger_type` TINYINT UNSIGNED NOT NULL,
  `storage_id` BIGINT NULL,
  `payload_sha256` BINARY(32) NULL,
  `event_key` VARCHAR(128) NULL,
  `message_version` BIGINT UNSIGNED NULL,
  `process_status` TINYINT UNSIGNED NOT NULL,
  `ingress_ack_status` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `ingress_ack_time` DATETIME(3) NULL,
  `redis_xack_status` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `failure_stage` VARCHAR(32) NULL,
  `error_code` INT NULL,
  `failure_reason` VARCHAR(1024) NULL,
  `started_time` DATETIME(3) NOT NULL,
  `finished_time` DATETIME(3) NOT NULL,
  `duration_ms` BIGINT UNSIGNED NOT NULL,
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  KEY `idx_event_time` (`event_id`, `create_time`),
  KEY `idx_event_success` (`event_id`, `process_status`),
  KEY `idx_status_time` (`process_status`, `create_time`),
  KEY `idx_storage` (`storage_id`, `payload_sha256`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pipeline订单事件处理审计流水';

CREATE TABLE `pipeline_payment_event_log` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `event_id` BIGINT NULL,
  `stream_record_id` VARCHAR(64) NULL,
  `trigger_type` TINYINT UNSIGNED NOT NULL,
  `storage_id` BIGINT NULL,
  `payload_sha256` BINARY(32) NULL,
  `event_key` VARCHAR(128) NULL,
  `message_version` BIGINT UNSIGNED NULL,
  `process_status` TINYINT UNSIGNED NOT NULL,
  `ingress_ack_status` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `ingress_ack_time` DATETIME(3) NULL,
  `redis_xack_status` TINYINT UNSIGNED NOT NULL DEFAULT 0,
  `failure_stage` VARCHAR(32) NULL,
  `error_code` INT NULL,
  `failure_reason` VARCHAR(1024) NULL,
  `started_time` DATETIME(3) NOT NULL,
  `finished_time` DATETIME(3) NOT NULL,
  `duration_ms` BIGINT UNSIGNED NOT NULL,
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  KEY `idx_event_time` (`event_id`, `create_time`),
  KEY `idx_event_success` (`event_id`, `process_status`),
  KEY `idx_status_time` (`process_status`, `create_time`),
  KEY `idx_storage` (`storage_id`, `payload_sha256`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pipeline支付事件处理审计流水';

-- 补拉租约
CREATE TABLE `pipeline_event_pull_control` (
  `content_type` TINYINT UNSIGNED NOT NULL,
  `lease_owner` VARCHAR(64) NULL,
  `lease_until` DATETIME(3) NULL,
  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`content_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pipeline未ACK事件批量补拉租约';

INSERT INTO `pipeline_event_pull_control` (`content_type`) VALUES (1), (2);

-- ============================================================
-- 5. 创建年度/分片物理表
-- ============================================================

-- 5.1 订单年度分表 (oms_order_2026, oms_order_2027)
CREATE TABLE `oms_order_2026` LIKE `oms_order`;
CREATE TABLE `oms_order_2027` LIKE `oms_order`;

-- 5.2 订单商品明细分表 (oms_order_item_YYYY_00 ~ _15)
DELIMITER //
CREATE PROCEDURE create_order_item_shards()
BEGIN
  DECLARE y INT DEFAULT 2026;
  DECLARE i INT DEFAULT 0;
  WHILE y <= 2027 DO
    SET i = 0;
    WHILE i < 16 DO
      SET @sql = CONCAT(
        'CREATE TABLE `oms_order_item_', y, '_', LPAD(i, 2, '0'), '` LIKE `oms_order_item`'
      );
      PREPARE stmt FROM @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;
      SET i = i + 1;
    END WHILE;
    SET y = y + 1;
  END WHILE;
END //
DELIMITER ;

CALL create_order_item_shards();
DROP PROCEDURE create_order_item_shards;

-- 5.3 订单规格明细年度分表
CREATE TABLE `oms_order_item_spec_2026` LIKE `oms_order_item_spec`;
CREATE TABLE `oms_order_item_spec_2027` LIKE `oms_order_item_spec`;

-- 5.4 订单套餐子商品年度分表
CREATE TABLE `oms_order_package_item_2026` LIKE `oms_order_package_item`;
CREATE TABLE `oms_order_package_item_2027` LIKE `oms_order_package_item`;

-- 5.5 支付年度分表
CREATE TABLE `oms_payment_2026` LIKE `oms_payment`;
CREATE TABLE `oms_payment_2027` LIKE `oms_payment`;

-- 5.6 支付结算账户年度分表
CREATE TABLE `oms_payment_account_2026` LIKE `oms_payment_account`;
CREATE TABLE `oms_payment_account_2027` LIKE `oms_payment_account`;

-- ============================================================
-- 完成
-- ============================================================
SELECT 'Fake databases created successfully!' AS result;
