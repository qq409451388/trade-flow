# 首页
```sql
SELECT
  SUM(CASE WHEN o.order_state != '99'
      THEN o.order_dis_amt ELSE 0 END) AS turnoverAmount,
  SUM(CASE WHEN o.order_state != '99'
      THEN o.pay_amt ELSE 0 END) AS revenueAmount,
  SUM(CASE WHEN o.order_state != '99'
      THEN o.coupon_amt
         + o.integral_deduction_amt
         + o.full_minus_amt
         + o.member_price_dis_amt
      ELSE 0 END) AS merchantSubsidy,
  SUM(CASE WHEN o.order_state != '99'
      THEN o.mchnt_express_cost
         + o.express_amt
         + o.lunch_box_fee
      ELSE 0 END) AS otherExpenditure,
  SUM(CASE WHEN o.order_state != '99'
      THEN o.free_consume_amt ELSE 0 END) AS storedValueGift,
  COUNT(DISTINCT CASE WHEN o.order_state != '99'
      THEN o.order_no END) AS validOrderCount,
  COUNT(DISTINCT CASE WHEN o.order_state = '99'
      THEN o.order_no END) AS refundOrderCount
FROM oms_order o
WHERE o.pay_time >= '2026-07-17 00:00:00'
AND o.pay_time < '2026-07-18 00:00:00';
```
# 营收看板
```sql
SELECT
  (v.turnover - v.expenditure) AS expectedIncome,
  v.turnover AS turnoverAmount,
  v.expenditure AS merchantExpenditure,
  v.validCnt AS validOrderCount,
  ROUND(
      IFNULL(
        (v.turnover - v.expenditure)
        / NULLIF(v.turnover,0),
        0
      ),
      4
  ) AS arrivalRate,
  ROUND(
      v.turnover / NULLIF(v.validCnt,0),
      2
  ) AS avgPreDiscount,
  ROUND(
      v.pay / NULLIF(v.validCnt,0),
      2
  ) AS avgPostDiscount,
  ROUND(
      v.turnover / NULLIF(v.guests,0),
      2
  ) AS avgPreDiscountPerGuest,
  ROUND(
      v.pay / NULLIF(v.validCnt,0),
      2
  ) AS avgCustomerSpending,
  v.guests AS guestCount,
  v.orderAmt AS originalPrice,
  v.lunchBox AS packingFee,
  v.selfDelivery AS selfDeliveryFee,
  v.discount AS discountAmount,
  0 AS platformServiceFee,
  v.partialRefund AS partialRefundAmt,
  v.fullRefund AS fullRefundAmt,
  v.freeConsume AS freeConsumeAmt
FROM (
  SELECT
    SUM(
      CASE WHEN o.order_state != '99'
      THEN o.order_dis_amt ELSE 0 END
    ) AS turnover,
    SUM(
      CASE WHEN o.order_state != '99'
      THEN o.pay_amt ELSE 0 END
    ) AS pay,
    SUM(
      CASE WHEN o.order_state != '99'
      THEN o.express_amt
         + o.mchnt_express_cost
         + o.lunch_box_fee
      ELSE 0 END
    ) AS expenditure,
    SUM(
      CASE WHEN o.order_state != '99'
      THEN o.guests_count ELSE 0 END
    ) AS guests,
    COUNT(DISTINCT
      CASE WHEN o.order_state != '99'
      THEN o.order_no END
    ) AS validCnt,
    SUM(
      CASE WHEN o.order_state != '99'
      THEN o.order_amt ELSE 0 END
    ) AS orderAmt,
    SUM(
      CASE WHEN o.order_state != '99'
      THEN o.lunch_box_fee ELSE 0 END
    ) AS lunchBox,
    SUM(
      CASE WHEN o.order_state != '99'
       AND o.express_company = '02'
      THEN o.express_amt ELSE 0 END
    ) AS selfDelivery,
    SUM(
      CASE WHEN o.order_state != '99'
      THEN o.coupon_amt
         + o.full_minus_amt
         + o.member_level_dis_amt
         + o.member_price_dis_amt
         + o.cashier_dis_amt
         + o.single_goods_dis_amt
         + o.group_pay_dis_amt
         + o.member_day_dis_amt
         + o.package_price_dis_amt
         + o.integral_deduction_amt
         + o.unionpay_dis_amt
         + o.times_card_dis_amt
         + o.wipe_zero_amt
      ELSE 0 END
    ) AS discount,
    SUM(
      CASE WHEN o.order_state != '99'
      THEN o.free_consume_amt ELSE 0 END
    ) AS freeConsume,
    SUM(
      CASE WHEN o.order_pay_state = 8
      THEN o.refund_amt ELSE 0 END
    ) AS partialRefund,
    SUM(
      CASE WHEN o.order_pay_state = 9
      THEN o.refund_amt ELSE 0 END
    ) AS fullRefund
  FROM oms_order o
  WHERE o.finish_time >= '2026-07-17 00:00:00'
  AND o.finish_time < '2026-07-18 00:00:00'
) v;
```
# 24h营业趋势
```sql
SELECT
  CONCAT(
      LPAD(h.h, 2, '0'),
      ':00-',
      LPAD(h.h + 1, 2, '0'),
      ':00'
  ) AS timeInterval,
  COALESCE(t.currentValue, 0) AS currentValue
FROM (
  SELECT 0 h UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
  UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
  UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11
  UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
  UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19
  UNION ALL SELECT 20 UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23
) h
LEFT JOIN (
  SELECT
    HOUR(o.order_create_time) AS h,
    SUM(o.order_dis_amt) AS currentValue
  FROM oms_order o
  WHERE o.order_create_time >= '2026-07-17 00:00:00'
    AND o.order_create_time < '2026-07-18 00:00:00'
    AND o.finish_time >= '2026-07-17 00:00:00'
    AND o.finish_time < '2026-07-18 00:00:00'
    AND o.order_state != '99'
  GROUP BY HOUR(o.order_create_time)
) t ON t.h = h.h
ORDER BY h.h;
```
# 时段分析
```sql
-- 早餐
SELECT
  '早餐' AS timePeriod,
  '7:00-10:00' AS timeRange,
  COALESCE(SUM(o.pay_amt),0) AS salesAmount,
  COUNT(DISTINCT o.order_no) AS orderCount
FROM oms_order o
WHERE o.order_create_time >= DATE_ADD('2026-07-17 00:00:00', INTERVAL 7 HOUR)
AND o.order_create_time < DATE_ADD('2026-07-17 00:00:00', INTERVAL 10 HOUR)
AND o.order_state != '99'
UNION ALL
-- 午餐
SELECT
  '午餐',
  '10:00-14:00',
  COALESCE(SUM(o.pay_amt),0),
  COUNT(DISTINCT o.order_no)
FROM oms_order o
WHERE o.order_create_time >= DATE_ADD('2026-07-17 00:00:00', INTERVAL 10 HOUR)
AND o.order_create_time < DATE_ADD('2026-07-17 00:00:00', INTERVAL 14 HOUR)
AND o.order_state != '99'
UNION ALL
-- 下午茶
SELECT
  '下午茶',
  '14:00-17:00',
  COALESCE(SUM(o.pay_amt),0),
  COUNT(DISTINCT o.order_no)
FROM oms_order o
WHERE o.order_create_time >= DATE_ADD('2026-07-17 00:00:00', INTERVAL 14 HOUR)
AND o.order_create_time < DATE_ADD('2026-07-17 00:00:00', INTERVAL 17 HOUR)
AND o.order_state != '99'
UNION ALL
-- 晚餐
SELECT
  '晚餐',
  '17:00-20:00',
  COALESCE(SUM(o.pay_amt),0),
  COUNT(DISTINCT o.order_no)
FROM oms_order o
WHERE o.order_create_time >= DATE_ADD('2026-07-17 00:00:00', INTERVAL 17 HOUR)
AND o.order_create_time < DATE_ADD('2026-07-17 00:00:00', INTERVAL 20 HOUR)
AND o.order_state != '99'
UNION ALL
-- 夜宵
SELECT
  '夜宵',
  '20:00-次日7:00',
  (
    COALESCE(
      SUM(
        CASE
          WHEN o.order_create_time >= DATE_ADD('2026-07-17 00:00:00', INTERVAL 20 HOUR)
           AND o.order_create_time < DATE_ADD('2026-07-18 00:00:00', INTERVAL 7 HOUR)
          THEN o.pay_amt
        END
      ),0)
  ),
  COUNT(DISTINCT
    CASE
      WHEN o.order_create_time >= DATE_ADD('2026-07-17 00:00:00', INTERVAL 20 HOUR)
       AND o.order_create_time < DATE_ADD('2026-07-18 00:00:00', INTERVAL 7 HOUR)
      THEN o.order_no
    END
  )
FROM oms_order o
WHERE o.order_create_time >= DATE_ADD('2026-07-17 00:00:00', INTERVAL 20 HOUR)
AND o.order_create_time < DATE_ADD('2026-07-18 00:00:00', INTERVAL 7 HOUR)
AND o.order_state != '99';
```
# 商品分析
```sql
SELECT
    i.goods_id AS goodsId,
    ANY_VALUE(i.goods_name) AS goodsName,
    SUM(i.goods_number) AS salesQuantity,
    SUM(
        CASE
            WHEN i.goods_real_pay_amt <> 0
            THEN i.goods_real_pay_amt
            ELSE i.goods_pay_amt
        END
    ) AS salesAmount
FROM oms_order o
INNER JOIN oms_order_item i
    ON o.order_no = i.order_no
WHERE o.order_create_time >= '2026-07-17 00:00:00'
AND o.order_create_time < '2026-07-18 00:00:00'
AND o.finish_time >= '2026-07-17 00:00:00'
AND o.finish_time < '2026-07-18 00:00:00'
AND i.dish_state = 1
AND o.order_state != '99'
GROUP BY i.goods_id
ORDER BY salesAmount DESC
LIMIT 100;
```
# 券核销汇总
```sql
SELECT
    COALESCE(SUM(o.group_pay_num),0)
      + COALESCE(SUM(o.group_pay_num_extra),0)
      AS totalVerifyCount,
    COALESCE(SUM(o.group_pay_dis_amt),0)
      AS totalDeductionAmount,
    COALESCE(SUM(o.pay_amt),0)
      AS totalDrivenAmount,
    COALESCE(SUM(o.group_pay_num),0)
      AS totalPackageVerifyCount,
    COALESCE(SUM(o.group_pay_dis_amt),0)
      AS totalPackageDeductionAmount
FROM oms_order o
WHERE o.finish_time >= '2026-07-17 00:00:00'
AND o.finish_time < '2026-07-18 00:00:00'
AND (
    o.group_pay_num
    + o.group_pay_dis_amt
    + o.group_pay_num_extra
) > 0
AND o.order_state != '99';
```
# 排行榜总收入
```sql
SELECT
    COALESCE(SUM(pay_amt),0) AS totalRevenue
FROM oms_order
WHERE finish_time >= '2026-07-17 00:00:00'
AND finish_time < '2026-07-18 00:00:00'
AND order_state != '99';
```
# 门店排行
```sql
SELECT
    o.shop_id AS shopId,
    SUM(o.pay_amt) AS revenue
FROM oms_order o
WHERE o.finish_time >= '2026-07-17 00:00:00'
AND o.finish_time < '2026-07-18 00:00:00'
AND o.order_state != '99'
GROUP BY o.shop_id
ORDER BY revenue DESC
LIMIT 50;
```
# 订单列表
```sql
SELECT
    o.order_no AS orderNo,
    o.third_order_no AS thirdOrderNo,
    o.channel_type AS orderSource,
    o.order_type AS orderType,
    o.order_state AS orderState,
    o.order_pay_state AS orderPayState,
    DATE_FORMAT(
            o.order_create_time,
            '%Y-%m-%d %H:%i:%s'
    ) AS crtTm,
    DATE_FORMAT(
            o.pay_time,
            '%Y-%m-%d %H:%i:%s'
    ) AS payTm,
    DATE_FORMAT(
            o.finish_time,
            '%Y-%m-%d %H:%i:%s'
    ) AS finishTm,
    o.shop_id AS shopId,
    p.shop_name AS shopName,
    o.order_dis_amt AS orderDisAmt,
    o.pay_amt AS payAmt,
    (
        o.pay_amt
            - o.express_amt
            - o.mchnt_express_cost
            - o.lunch_box_fee
        ) AS expectedIncome,
    (
        o.coupon_amt
            + o.full_minus_amt
            + o.member_level_dis_amt
            + o.member_price_dis_amt
            + o.full_order_dis_amt
            + o.single_goods_dis_amt
            + o.group_pay_dis_amt
            + o.member_day_dis_amt
            + o.package_price_dis_amt
            + o.integral_deduction_amt
            + o.unionpay_dis_amt
            + o.times_card_dis_amt
            + o.wipe_zero_amt
        ) AS discountAmount,
    o.user_memo AS userMemo,
    CASE
        WHEN o.phone = ''
            THEN NULL
        ELSE CONCAT(
            LEFT(o.phone,3),
                '****',
            RIGHT(o.phone,4)
        )
        END AS phoneMasked
FROM oms_order o
         LEFT JOIN (
    SELECT
        order_no,
        MAX(shop_name) AS shop_name
    FROM oms_payment
    WHERE order_no != 0
    GROUP BY order_no
) p
ON p.order_no = o.order_no
WHERE o.order_create_time >= '2026-07-10 00:00:00'
  AND o.order_create_time < '2026-07-11 00:00:00'
  AND o.order_state IN ('03','04','05')
ORDER BY o.order_create_time DESC
    LIMIT 20 OFFSET 0;
```
