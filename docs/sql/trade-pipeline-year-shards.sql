-- Trade Pipeline 2026、2027 年物理分表。
-- 前置条件：已执行 trade-pipeline-base-schema.sql，基础表仅作为 LIKE 模板。
-- 本文件可重复执行；新增年度时复制对应年份段并同步 trade.pipeline.sharding.years。

-- 2026 年订单表。
CREATE TABLE IF NOT EXISTS `oms_order_2026` LIKE `oms_order`;
CREATE TABLE IF NOT EXISTS `oms_order_item_spec_2026` LIKE `oms_order_item_spec`;
CREATE TABLE IF NOT EXISTS `oms_order_package_item_2026` LIKE `oms_order_package_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_00` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_01` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_02` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_03` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_04` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_05` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_06` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_07` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_08` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_09` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_10` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_11` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_12` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_13` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_14` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2026_15` LIKE `oms_order_item`;

-- 2026 年支付表。
CREATE TABLE IF NOT EXISTS `oms_payment_2026` LIKE `oms_payment`;
CREATE TABLE IF NOT EXISTS `oms_payment_account_2026` LIKE `oms_payment_account`;

-- 2027 年订单表。
CREATE TABLE IF NOT EXISTS `oms_order_2027` LIKE `oms_order`;
CREATE TABLE IF NOT EXISTS `oms_order_item_spec_2027` LIKE `oms_order_item_spec`;
CREATE TABLE IF NOT EXISTS `oms_order_package_item_2027` LIKE `oms_order_package_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_00` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_01` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_02` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_03` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_04` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_05` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_06` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_07` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_08` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_09` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_10` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_11` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_12` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_13` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_14` LIKE `oms_order_item`;
CREATE TABLE IF NOT EXISTS `oms_order_item_2027_15` LIKE `oms_order_item`;

-- 2027 年支付表。
CREATE TABLE IF NOT EXISTS `oms_payment_2027` LIKE `oms_payment`;
CREATE TABLE IF NOT EXISTS `oms_payment_account_2027` LIKE `oms_payment_account`;
