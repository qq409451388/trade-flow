-- Trade Pipeline 最终基础结构。
-- oms_* 无年份表仅作为年度物理表模板；运行时 ShardingSphere 不路由到这些模板表。
-- 新环境执行顺序：先执行本文件，再执行 trade-pipeline-year-shards.sql。

CREATE TABLE `oms_order` (
                             `id` BIGINT NOT NULL COMMENT '全局雪花ID',
                             `storage_id` BIGINT NOT NULL COMMENT 'Storage 原文ID',
                             `payload_sha256` BINARY(32) NOT NULL COMMENT 'Storage 原文SHA-256',

                             `order_no` BIGINT NOT NULL COMMENT '富友订单号',
                             `user_id` BIGINT DEFAULT NULL COMMENT '用户ID',
                             `mchnt_cd` VARCHAR(32) NOT NULL DEFAULT '' COMMENT '富友商户号',
                             `shop_id` BIGINT DEFAULT NULL COMMENT '门店ID',

                             `express_id` BIGINT DEFAULT NULL COMMENT '快递单ID',
                             `tm_fuiou_id` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '桌贴号或终端号',
                             `term_name` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '桌贴名称或终端别名',

                             `order_type` VARCHAR(4) NOT NULL DEFAULT ''
                                 COMMENT '订单类型：01小程序堂食；02外卖；03收银机支付',

                             `channel_type` VARCHAR(4) NOT NULL DEFAULT ''
                                 COMMENT '渠道类型',

                             `order_state` VARCHAR(4) NOT NULL DEFAULT ''
                                 COMMENT '订单状态',

                             `order_pay_state` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '支付状态：1已支付；8部分退款；9已退款',

                             `express_state` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '配送状态，历史存在0、1、2、3、4、8、9',

                             `order_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '商品打折前金额（分）',

                             `order_dis_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '订单原单金额（分）',

                             `pay_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '实际支付金额（分）',

                             `pay_amt_extra` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '额外支付金额（分）',

                             `cash_received_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '现金实收金额（分）',

                             `refund_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '累计退款金额（分）',

                             `express_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '快递费（分）',

                             `mchnt_express_cost` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '配送商实收快递费（分）',

                             `lunch_box_fee` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '餐盒费（分）',

                             `invoice_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '开票金额（分）',

                             `third_mchnt_income` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '第三方外卖商家预计收入（分）',

                             `order_create_time` DATETIME(3) NOT NULL
        COMMENT '订单创建时间，年度分表依据，来源crtTm',

                             `pay_deadline_time` DATETIME(3) DEFAULT NULL
        COMMENT '最晚支付时间',

                             `pay_time` DATETIME(3) DEFAULT NULL
        COMMENT '订单支付时间',

                             `cashier_confirm_time` DATETIME(3) DEFAULT NULL
        COMMENT '收银员确认时间',

                             `delivery_start_time` DATETIME(3) DEFAULT NULL
        COMMENT '开始配送时间',

                             `comment_time` DATETIME(3) DEFAULT NULL
        COMMENT '评价时间',

                             `finish_time` DATETIME(3) DEFAULT NULL
        COMMENT '订单完成时间',

                             `source_update_time` DATETIME(3) DEFAULT NULL
        COMMENT '富友订单最近更新时间，来源recUpdTm',

                             `refund_time` DATETIME(3) DEFAULT NULL
        COMMENT '退款时间',

                             `open_table_time` DATETIME(3) DEFAULT NULL
        COMMENT '开台时间',

                             `meal_time` DATETIME(3) DEFAULT NULL
        COMMENT '就餐时间',

                             `estimated_finish_time` DATETIME(3) DEFAULT NULL
        COMMENT '预计完成时间',

                             `reverse_time` DATETIME(3) DEFAULT NULL
        COMMENT '反结账时间',

                             `deliver_time_desc` VARCHAR(64) NOT NULL DEFAULT ''
                                 COMMENT '期望送达时间原始描述',

                             `pay_ssn` VARCHAR(64) NOT NULL DEFAULT ''
                                 COMMENT '支付流水号',

                             `pay_type` VARCHAR(32) NOT NULL DEFAULT ''
                                 COMMENT '主要支付方式',

                             `pay_type_extra` VARCHAR(32) NOT NULL DEFAULT ''
                                 COMMENT '额外支付方式',

                             `third_order_no` VARCHAR(64) NOT NULL DEFAULT ''
                                 COMMENT '第三方平台订单号',

                             `app_open_id` VARCHAR(128) NOT NULL DEFAULT ''
                                 COMMENT '用户OpenID',

                             `out_user_id` VARCHAR(64) NOT NULL DEFAULT ''
                                 COMMENT '外部用户ID',

                             `meal_code` VARCHAR(32) NOT NULL DEFAULT ''
                                 COMMENT '取餐码',

                             `user_memo` VARCHAR(512) NOT NULL DEFAULT ''
                                 COMMENT '用户备注',

                             `order_cancel_reason` VARCHAR(512) NOT NULL DEFAULT ''
                                 COMMENT '订单取消原因',

                             `order_comment` VARCHAR(1024) NOT NULL DEFAULT ''
                                 COMMENT '订单评价内容',

                             `comment_state` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '评价状态，历史存在0、9',

                             `comment_level` VARCHAR(4) NOT NULL DEFAULT ''
                                 COMMENT '评价星级',

                             `order_addr_id` BIGINT DEFAULT NULL
                                 COMMENT '用户收货地址ID',

                             `phone` VARCHAR(20) NOT NULL DEFAULT ''
                                 COMMENT '收货人手机号',

                             `contact_mobile` VARCHAR(20) NOT NULL DEFAULT ''
                                 COMMENT '联系人手机号',

                             `express_company` VARCHAR(4) NOT NULL DEFAULT ''
                                 COMMENT '配送商：01达达；02自配送',

                             `cashier_id` VARCHAR(32) NOT NULL DEFAULT ''
                                 COMMENT '收银员账号',

                             `cashier_dis_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '收银员整单打折金额（分）',

                             `single_goods_dis_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '单品手工打折金额（分）',

                             `full_order_dis_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '整单手工优惠总额（分）',

                             `cashier_discount` DECIMAL(7,2) DEFAULT NULL
                                 COMMENT '整单折扣值，历史范围0.00至2800.00',

                             `cashier_dis_id` BIGINT DEFAULT NULL
                                 COMMENT '手工折扣ID',

                             `cashier_dis_name` VARCHAR(64) NOT NULL DEFAULT ''
                                 COMMENT '手工折扣名称',

                             `discount_type` VARCHAR(4) NOT NULL DEFAULT ''
                                 COMMENT '主要优惠类型',

                             `discount_type_extra` VARCHAR(4) NOT NULL DEFAULT ''
                                 COMMENT '额外优惠类型',

                             `coupon_id` BIGINT DEFAULT NULL COMMENT '优惠券ID',
                             `coupon_real_id` BIGINT DEFAULT NULL COMMENT '优惠券真实ID',

                             `special_coupon_id` VARCHAR(64) NOT NULL DEFAULT ''
                                 COMMENT '特殊商户优惠券ID',

                             `special_mchnt_type` VARCHAR(4) NOT NULL DEFAULT ''
                                 COMMENT '特殊商户类型',

                             `integral` BIGINT NOT NULL DEFAULT 0 COMMENT '抵扣积分数',

                             `integral_deduction_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '积分优惠金额（分）',

                             `coupon_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '优惠券优惠金额（分）',

                             `full_minus_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '满减优惠金额（分）',

                             `member_level_dis_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '会员等级优惠金额（分）',

                             `member_price_dis_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '会员价优惠金额（分）',

                             `member_day_dis_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '会员日优惠金额（分）',

                             `unionpay_dis_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '银联优惠金额（分）',

                             `times_card_dis_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '次卡优惠金额（分）',

                             `package_price_dis_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '套餐折扣金额（分）',

                             `group_pay_dis_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '团购券优惠金额（分）',

                             `plat_hong_bao` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '第三方平台红包金额（分）',

                             `wipe_zero_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '抹零金额（分），允许负数',

                             `not_in_discount_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '不参与优惠金额（分）',

                             `free_consume_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '会员赠送账户消费金额（分）',

                             `fixed_price_amt` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '固价商品支付总额（分）',

                             `group_pay_num` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '团购券使用数量',

                             `group_pay_num_extra` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '额外团购券使用数量',

                             `is_membership` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否会员：0否；1是',

                             `is_meal_order` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否重餐订单：0否；1是',

                             `is_order_locked` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '订单是否锁定：0否；1是',

                             `has_reverse` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否曾经反结账：0否；1是',

                             `is_pad_confirm` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否PAD确认：0否；1是',

                             `is_account_order` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否挂账订单：0否；1是',

                             `print_settle_status` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '结算单打印状态，历史存在0、4',

                             `third_basket_status` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '第三方篮子状态，历史存在0、1、2、3、5',

                             `invoice_state` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '开票状态',

                             `mqtt_send_state` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT 'MQTT推送状态，历史存在0、1、2、3',

                             `meal_confirm_channel` VARCHAR(4) NOT NULL DEFAULT ''
                                 COMMENT '重餐确认渠道',

                             `table_fuiou_id` VARCHAR(64) NOT NULL DEFAULT ''
                                 COMMENT '餐桌终端号',

                             `table_term_name` VARCHAR(128) NOT NULL DEFAULT ''
                                 COMMENT '餐桌桌贴名称',

                             `promoter_no` VARCHAR(32) NOT NULL DEFAULT ''
                                 COMMENT '推广员编号',

                             `account_memo` VARCHAR(512) NOT NULL DEFAULT ''
                                 COMMENT '挂账备注',

                             `member_phone` VARCHAR(20) NOT NULL DEFAULT ''
                                 COMMENT '会员手机号',

                             `member_name` VARCHAR(64) NOT NULL DEFAULT ''
                                 COMMENT '会员名称',

                             `member_points` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '商品会员价所需积分',

                             `user_balance` BIGINT NOT NULL DEFAULT 0
                                 COMMENT '订单消费后的会员账户余额（分）',

                             `guests_count` SMALLINT DEFAULT NULL
                                 COMMENT '就餐人数，历史范围-10至10',

                             `order_version` SMALLINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '富友订单版本号',

                             `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        COMMENT '本地首次入库时间',

                             `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3)
        COMMENT '本地最近更新时间',

                             PRIMARY KEY (`id`),

                             UNIQUE KEY `uk_order_no` (`order_no`),

                             KEY `idx_shop_create_time`
                                 (`mchnt_cd`, `shop_id`, `order_create_time`),

                             KEY `idx_state_create_time`
                                 (`mchnt_cd`, `order_state`, `order_create_time`),

                             KEY `idx_pay_ssn` (`pay_ssn`),
                             KEY `idx_third_order_no` (`third_order_no`),
                             KEY `idx_source_update_time` (`source_update_time`),
                             KEY `idx_update_time` (`update_time`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  ROW_FORMAT = DYNAMIC
  COMMENT = '富友订单当前快照基础表，实际按订单创建年份建表';


CREATE TABLE `oms_order_item` (
                                  `id` BIGINT NOT NULL COMMENT '全局雪花ID',

                                  `detail_no` BIGINT NOT NULL
                                      COMMENT '富友订单商品明细唯一编号',

                                  `order_no` BIGINT NOT NULL
                                      COMMENT '关联订单号，同时作为Hash分片键',

                                  `shop_id` BIGINT NOT NULL COMMENT '门店ID',

                                  `mchnt_cd` VARCHAR(32) NOT NULL DEFAULT ''
                                      COMMENT '富友商户号',

                                  `goods_id` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '商品ID，0表示未关联商品或特殊商品',

                                  `goods_name` VARCHAR(128) NOT NULL DEFAULT ''
                                      COMMENT '商品名称',

                                  `goods_unit` VARCHAR(32) NOT NULL DEFAULT ''
                                      COMMENT '库存单位',

                                  `goods_bar_code` VARCHAR(64) NOT NULL DEFAULT ''
                                      COMMENT '商品条码',

                                  `goods_base_price` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '商品基础单价（分）',

                                  `goods_dis_price` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '商品折扣单价（分）',

                                  `goods_price` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '商品总价（分）',

                                  `goods_number` DECIMAL(14,3) NOT NULL DEFAULT 0.000
                                      COMMENT '购买数量，允许负数表示冲减明细',

                                  `refund_goods_number` DECIMAL(14,3) NOT NULL DEFAULT 0.000
                                      COMMENT '累计退款商品数量',

                                  `goods_pay_amt` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '单品实付分摊金额（分），允许负数',

                                  `goods_real_pay_amt` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '单品真实分摊支付金额（分）',

                                  `goods_total_refund_amt` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '单品累计退款或冲减金额（分），允许负数',

                                  `cashier_dis_price` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '单品手工折扣后单价（分）',

                                  `goods_cashier_discount` DECIMAL(5,2) NOT NULL DEFAULT 0.00
                                      COMMENT '单品手工折扣比例',

                                  `dis_discount_reason` VARCHAR(64) NOT NULL DEFAULT ''
                                      COMMENT '单品打折原因',

                                  `goods_dis_type` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '单品折扣类型：0无；1手工折扣；2会员价',

                                  `goods_member_price` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '会员总价（分）',

                                  `goods_member_dis_amt` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '会员优惠金额（分）',

                                  `goods_member_points` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '单品会员价所需积分',

                                  `promotion_id` BIGINT DEFAULT NULL
                                      COMMENT '促销活动ID',

                                  `goods_promotion_way` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '促销角色：0无；1发起方；2优惠方',

                                  `unionpay_coupon_id` VARCHAR(64) NOT NULL DEFAULT ''
                                      COMMENT '银联单品券ID',

                                  `avg_purchase_price` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '平均进货价（分）',

                                  `goods_sales_commission` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '销售提成比例，范围0至100',

                                  `goods_employee_commission` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '员工提成比例，范围0至100',

                                  `spec_id_list` VARCHAR(128) NOT NULL DEFAULT ''
                                      COMMENT '规格ID集合，逗号分隔',

                                  `spec_desc_list` VARCHAR(128) NOT NULL DEFAULT ''
                                      COMMENT '规格描述集合',

                                  `is_package_goods` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否套餐商品：0否；1是',

                                  `in_pkg_goods_price` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '套餐内单品原价总额（分）',

                                  `goods_basket` VARCHAR(16) NOT NULL DEFAULT ''
                                      COMMENT '所属篮子',

                                  `goods_lunch_box_fee` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '单品餐盒费（分）',

                                  `prepackaged_amt` BIGINT NOT NULL DEFAULT 0
                                      COMMENT '预包装商品总额（分）',

                                  `is_third_order` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否第三方订单：0否；1是',

                                  `add_dish_channel` VARCHAR(4) NOT NULL DEFAULT ''
                                      COMMENT '加菜渠道：00非加菜；01加菜',

                                  `dish_state` TINYINT UNSIGNED NOT NULL DEFAULT 1
        COMMENT '菜品状态：0取消；1正常；2延后上菜',

                                  `dish_user_memo` VARCHAR(128) NOT NULL DEFAULT ''
                                      COMMENT '菜品用户备注',

                                  `dish_cashier_memo` VARCHAR(128) NOT NULL DEFAULT ''
                                      COMMENT '收银员菜品备注',

                                  `dish_cancel_reason` VARCHAR(256) NOT NULL DEFAULT ''
                                      COMMENT '菜品取消原因',

                                  `dish_fuiou_id` VARCHAR(32) NOT NULL DEFAULT ''
                                      COMMENT '点餐终端号',

                                  `dish_user_id` BIGINT DEFAULT NULL
                                      COMMENT '点餐用户ID',

                                  `dish_cashier_id` VARCHAR(32) NOT NULL DEFAULT ''
                                      COMMENT '操作收银员账号',

                                  `dish_has_hurried` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否催菜：0否；1是',

                                  `dish_has_finish` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否已上菜：0否；1是',

                                  `is_dish_confirm` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '菜品是否确认：0否；1是',

                                  `is_opr_by_pad` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否由PAD操作：0否；1是',

                                  `is_weigh_goods` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否称重商品：0否；1是',

                                  `mark_print` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '后厨打印标记：0不打印；1打印',

                                  `dish_print_state` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否已打印厨打单：0否；1是',

                                  `kitchen_print` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '上游后厨打印标记',

                                  `print_serial_no` VARCHAR(64) NOT NULL DEFAULT ''
                                      COMMENT '打印机序列号',

                                  `calc_model` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '金额计算模式',

                                  `item_create_time` DATETIME(3) NOT NULL
        COMMENT '订单商品明细创建时间，来源crtTm',

                                  `dish_update_time` DATETIME(3) NOT NULL
        COMMENT '菜品最近更新时间，来源dishUpdTm',

                                  `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        COMMENT '本地首次入库时间',

                                  `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3)
        COMMENT '本地最近更新时间',

                                  PRIMARY KEY (`id`),

                                  UNIQUE KEY `uk_detail_no` (`detail_no`),

                                  KEY `idx_order_no` (`order_no`),
                                  KEY `idx_goods_id` (`goods_id`),

                                  KEY `idx_shop_item_time`
                                      (`mchnt_cd`, `shop_id`, `item_create_time`),

                                  KEY `idx_update_time` (`update_time`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  ROW_FORMAT = DYNAMIC
  COMMENT = '订单商品明细基础表，实际按年份和order_no模16分表';

CREATE TABLE `oms_order_item_spec` (
                                       `id` BIGINT NOT NULL
                                           COMMENT '全局雪花ID',

                                       `order_spec_id` BIGINT NOT NULL
                                           COMMENT '富友订单规格明细唯一编号',

                                       `detail_no` BIGINT NOT NULL
                                           COMMENT '关联订单商品明细编号',

                                       `spec_id` BIGINT NOT NULL DEFAULT 0
                                           COMMENT '规格ID，0表示上游未提供',

                                       `spec_name` VARCHAR(64) NOT NULL DEFAULT ''
                                           COMMENT '规格名称',

                                       `spec_detail_id` BIGINT NOT NULL DEFAULT 0
                                           COMMENT '规格值ID，0表示上游未提供',

                                       `spec_detail_desc` VARCHAR(128) NOT NULL DEFAULT ''
                                           COMMENT '规格值名称',

                                       `detail_extra_price` BIGINT NOT NULL DEFAULT 0
                                           COMMENT '规格加价金额（分）',

                                       `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        COMMENT '本地创建时间',

                                       `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3)
        COMMENT '本地更新时间',

                                       PRIMARY KEY (`id`),

                                       UNIQUE KEY `uk_order_spec_id` (`order_spec_id`),

                                       KEY `idx_detail_no` (`detail_no`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '订单商品规格明细基础表，实际跟随父订单按年份建表';

CREATE TABLE `oms_order_package_item` (
                                          `id` BIGINT NOT NULL
                                              COMMENT '全局雪花ID',

                                          `package_detail_no` BIGINT NOT NULL
                                              COMMENT '富友套餐商品明细唯一编号',

                                          `relate_pkg_detail_no` BIGINT NOT NULL
                                              COMMENT '关联主商品明细detail_no',

                                          `order_no` BIGINT NOT NULL
                                              COMMENT '关联订单号',

                                          `goods_id` BIGINT NOT NULL DEFAULT 0
                                              COMMENT '商品ID，0表示上游未提供或特殊商品',

                                          `shop_id` BIGINT NOT NULL
                                              COMMENT '门店ID',

                                          `mchnt_cd` VARCHAR(32) NOT NULL DEFAULT ''
                                              COMMENT '富友商户号',

                                          `goods_name` VARCHAR(128) NOT NULL DEFAULT ''
                                              COMMENT '商品名称',

                                          `goods_unit` VARCHAR(32) NOT NULL DEFAULT ''
                                              COMMENT '商品单位',

                                          `goods_bar_code` VARCHAR(64) NOT NULL DEFAULT ''
                                              COMMENT '商品条码',

                                          `goods_base_price` BIGINT NOT NULL DEFAULT 0
                                              COMMENT '商品基础单价（分）',

                                          `goods_dis_price` BIGINT NOT NULL DEFAULT 0
                                              COMMENT '商品折扣单价（分）',

                                          `goods_member_price` BIGINT NOT NULL DEFAULT 0
                                              COMMENT '会员总价（分）',

                                          `goods_member_dis_amt` BIGINT NOT NULL DEFAULT 0
                                              COMMENT '单品会员优惠金额（分）',

                                          `goods_cashier_discount` DECIMAL(7,2) NOT NULL DEFAULT 0.00
                                              COMMENT '单品手工折扣值，历史范围0.00至12560.00',

                                          `cashier_dis_price` BIGINT NOT NULL DEFAULT 0
                                              COMMENT '手工折扣后单价（分）',

                                          `goods_price` BIGINT NOT NULL DEFAULT 0
                                              COMMENT '商品总价（分）',

                                          `goods_pay_amt` BIGINT NOT NULL DEFAULT 0
                                              COMMENT '单品分摊实付金额（分）',

                                          `goods_number` DECIMAL(14,3) NOT NULL DEFAULT 0.000
                                              COMMENT '实际购买数量',

                                          `package_number` DECIMAL(14,3) NOT NULL DEFAULT 0.000
                                              COMMENT '单个套餐内该商品标准份数',

                                          `pkg_goods_copies` DECIMAL(14,3) NOT NULL DEFAULT 0.000
                                              COMMENT '套餐内该商品实际选购份数',

                                          `pkg_goods_extra_price` BIGINT NOT NULL DEFAULT 0
                                              COMMENT '套餐可选商品加价金额（分）',

                                          `avg_purchase_price` BIGINT NOT NULL DEFAULT 0
                                              COMMENT '平均进货价（分）',

                                          `goods_lunch_box_fee` BIGINT NOT NULL DEFAULT 0
                                              COMMENT '商品餐盒费（分）',

                                          `goods_member_points` BIGINT NOT NULL DEFAULT 0
                                              COMMENT '商品会员价所需积分',

                                          `goods_index` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '套餐商品排序序号',

                                          `mark_print` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否打印：0否；1是',

                                          `is_weigh_goods` TINYINT UNSIGNED NOT NULL DEFAULT 0
        COMMENT '是否称重商品：0否；1是',

                                          `spec_id_list` VARCHAR(64) NOT NULL DEFAULT ''
                                              COMMENT '规格ID集合，逗号分隔',

                                          `spec_desc_list` VARCHAR(128) NOT NULL DEFAULT ''
                                              COMMENT '规格描述集合',

                                          `third_detail_no` VARCHAR(64) NOT NULL DEFAULT ''
                                              COMMENT '第三方商品明细号',

                                          `package_dish_memo` VARCHAR(128) NOT NULL DEFAULT ''
                                              COMMENT '套餐商品备注',

                                          `item_create_time` DATETIME(3) NOT NULL
        COMMENT '套餐商品明细创建时间，来源crtTm',

                                          `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        COMMENT '本地创建时间',

                                          `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3)
        COMMENT '本地更新时间',

                                          PRIMARY KEY (`id`),

                                          UNIQUE KEY `uk_package_detail_no`
                                              (`package_detail_no`),

                                          KEY `idx_relate_pkg_detail_no`
                                              (`relate_pkg_detail_no`),

                                          KEY `idx_order_no` (`order_no`),

                                          KEY `idx_goods_id` (`goods_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  ROW_FORMAT = DYNAMIC
  COMMENT = '订单套餐子商品明细基础表，实际跟随父订单按年份建表';


CREATE TABLE `oms_payment` (
                                    `id` BIGINT UNSIGNED NOT NULL COMMENT '雪花主键',

                                    `pay_ssn` VARCHAR(64) NOT NULL COMMENT '支付或退款流水号',
                                    `source_pay_ssn` VARCHAR(64) NOT NULL DEFAULT ''
                                        COMMENT '退款对应的原支付流水号',

                                    `order_no` BIGINT NOT NULL DEFAULT 0
                                        COMMENT '订单号，0表示充值等非普通订单业务',

                                    `mchnt_cd` VARCHAR(32) NOT NULL COMMENT '商户号',
                                    `shop_id` BIGINT NOT NULL DEFAULT 0 COMMENT '门店ID，0表示无具体门店',
                                    `shop_name` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '门店名称快照',

                                    `pay_time` DATETIME(3) NOT NULL COMMENT '原支付时间，也是年度分表依据',
                                    `refund_time` DATETIME(3) DEFAULT NULL COMMENT '退款发生时间',

                                    `pay_type` VARCHAR(16) NOT NULL DEFAULT '' COMMENT '支付方式编码',
                                    `pay_name` VARCHAR(32) NOT NULL DEFAULT '' COMMENT '支付方式名称',
                                    `pay_state` TINYINT UNSIGNED NOT NULL COMMENT '1支付成功，2退款成功',

                                    `pay_amt` BIGINT NOT NULL DEFAULT 0 COMMENT '支付金额（分）',
                                    `fee_amt` BIGINT NOT NULL DEFAULT 0 COMMENT '手续费（分）',
                                    `refund_amt` BIGINT NOT NULL DEFAULT 0 COMMENT '退款金额（分）',
                                    `balance_dis_amt` BIGINT NOT NULL DEFAULT 0 COMMENT '赠送账户优惠金额（分）',
                                    `face_amt` BIGINT NOT NULL DEFAULT 0 COMMENT '券面金额（分）',

                                    `fy_settle` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '是否富友清算',

                                    `third_order_no` VARCHAR(64) NOT NULL DEFAULT ''
                                        COMMENT '第三方业务订单号，可被多条支付退款流水共用',
                                    `channel_trade_no` VARCHAR(64) NOT NULL DEFAULT ''
                                        COMMENT '支付渠道交易流水号',

                                    `big_category` VARCHAR(4) NOT NULL DEFAULT '' COMMENT '业务大类',
                                    `small_category` VARCHAR(4) NOT NULL DEFAULT '' COMMENT '业务小类',
                                    `order_source` VARCHAR(4) NOT NULL DEFAULT '' COMMENT '支付单来源',

                                    `open_id` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '支付用户OpenID，上游扩展字段',
                                    `order_type` VARCHAR(4) NOT NULL DEFAULT '' COMMENT '订单类型，上游扩展字段',
                                    `channel_type` VARCHAR(4) NOT NULL DEFAULT '' COMMENT '渠道类型，上游扩展字段',

                                    `member_name` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '会员名称快照',
                                    `phone` VARCHAR(20) NOT NULL DEFAULT '' COMMENT '会员手机号快照',
                                    `member_card_no` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '会员实体卡号',
                                    `member_level` VARCHAR(4) NOT NULL DEFAULT '' COMMENT '会员等级值',

                                    `storage_id` BIGINT UNSIGNED NOT NULL COMMENT '原始报文Storage ID',
                                    `payload_sha256` BINARY(32) NOT NULL COMMENT '原始报文SHA-256',

                                    `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                    `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),

                                    PRIMARY KEY (`id`),
                                    UNIQUE KEY `uk_pay_ssn` (`pay_ssn`),

                                    KEY `idx_source_pay_ssn` (`source_pay_ssn`),
                                    KEY `idx_order_no` (`order_no`),
                                    KEY `idx_third_order_no` (`third_order_no`),
                                    KEY `idx_channel_trade_no` (`channel_trade_no`),
                                    KEY `idx_pay_time` (`pay_time`),
                                    KEY `idx_mchnt_shop_time` (`mchnt_cd`, `shop_id`, `pay_time`),
                                    KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='支付及退款流水表';


CREATE TABLE `oms_payment_account` (
                                            `id` BIGINT UNSIGNED NOT NULL COMMENT '雪花主键',
                                            `payment_id` BIGINT UNSIGNED NOT NULL COMMENT '关联oms_payment.id',
                                            `pay_ssn` VARCHAR(64) NOT NULL COMMENT '关联支付或退款流水号',
                                            `account_seq` SMALLINT UNSIGNED NOT NULL COMMENT '结算账户列表序号',

                                            `cust_acnt_tp` VARCHAR(4) NOT NULL DEFAULT ''
                                                COMMENT '账户性质：G对公，S对私',
                                            `mchnt_cd` VARCHAR(32) NOT NULL DEFAULT '' COMMENT '结算商户号',
                                            `shop_id` BIGINT NOT NULL DEFAULT 0 COMMENT '结算门店ID',
                                            `out_acnt_nm` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '结算户名',
                                            `out_acnt_no` VARCHAR(128) NOT NULL DEFAULT ''
                                                COMMENT '上游返回的加密卡号',

                                            `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                                            `update_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),

                                            PRIMARY KEY (`id`),
                                            UNIQUE KEY `uk_payment_account_seq` (`payment_id`, `account_seq`),

                                            KEY `idx_pay_ssn` (`pay_ssn`),
                                            KEY `idx_mchnt_shop` (`mchnt_cd`, `shop_id`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='支付结算账户明细表';

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
    `ingress_ack_status` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Ingress ACK：0未执行；1成功；2失败',
    `ingress_ack_time` DATETIME(3) NULL COMMENT 'Ingress ACK成功时间',
    `redis_xack_status` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Redis XACK：0未执行；1成功；2失败；3不适用',
    `failure_stage` VARCHAR(32) NULL COMMENT '失败阶段',
    `error_code` INT NULL COMMENT '失败错误码',
    `failure_reason` VARCHAR(1024) NULL COMMENT '失败原因摘要',
    `started_time` DATETIME(3) NOT NULL COMMENT '开始处理时间',
    `finished_time` DATETIME(3) NOT NULL COMMENT '结束处理时间',
    `duration_ms` BIGINT UNSIGNED NOT NULL COMMENT '处理耗时毫秒',
    `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    KEY `idx_event_time` (`event_id`, `create_time`),
    KEY `idx_event_success` (`event_id`, `process_status`),
    KEY `idx_status_time` (`process_status`, `create_time`),
    KEY `idx_storage` (`storage_id`, `payload_sha256`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pipeline订单事件每次实际处理审计流水';

CREATE TABLE `pipeline_payment_event_log` (
    `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `event_id` BIGINT NULL COMMENT 'Ingress支付event ID，消息无法解析时为空',
    `stream_record_id` VARCHAR(64) NULL COMMENT 'Redis Stream record ID，主动拉取时为空',
    `trigger_type` TINYINT UNSIGNED NOT NULL COMMENT '触发方式：1新消息；2 PEL接管；3主动拉取',
    `storage_id` BIGINT NULL COMMENT '关联trade_storage.id',
    `payload_sha256` BINARY(32) NULL COMMENT 'Storage SHA-256',
    `event_key` VARCHAR(128) NULL COMMENT '第三方事件键',
    `message_version` BIGINT UNSIGNED NULL COMMENT '第三方消息版本',
    `process_status` TINYINT UNSIGNED NOT NULL COMMENT '结果：1已应用；2幂等重复；3失败',
    `ingress_ack_status` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Ingress ACK：0未执行；1成功；2失败',
    `ingress_ack_time` DATETIME(3) NULL COMMENT 'Ingress ACK成功时间',
    `redis_xack_status` TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Redis XACK：0未执行；1成功；2失败；3不适用',
    `failure_stage` VARCHAR(32) NULL COMMENT '失败阶段',
    `error_code` INT NULL COMMENT '失败错误码',
    `failure_reason` VARCHAR(1024) NULL COMMENT '失败原因摘要',
    `started_time` DATETIME(3) NOT NULL COMMENT '开始处理时间',
    `finished_time` DATETIME(3) NOT NULL COMMENT '结束处理时间',
    `duration_ms` BIGINT UNSIGNED NOT NULL COMMENT '处理耗时毫秒',
    `create_time` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    KEY `idx_event_time` (`event_id`, `create_time`),
    KEY `idx_event_success` (`event_id`, `process_status`),
    KEY `idx_status_time` (`process_status`, `create_time`),
    KEY `idx_storage` (`storage_id`, `payload_sha256`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pipeline支付事件每次实际处理审计流水';

CREATE TABLE `pipeline_event_pull_control` (
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
  COMMENT='Pipeline未ACK事件批量补拉租约';

INSERT INTO `pipeline_event_pull_control` (`content_type`)
VALUES (1), (2);
