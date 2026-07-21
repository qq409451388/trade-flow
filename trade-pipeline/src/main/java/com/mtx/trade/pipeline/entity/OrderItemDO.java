package com.mtx.trade.pipeline.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/** 订单商品明细。 */
@Data
@TableName("oms_order_item")
public class OrderItemDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.INPUT)
    private Long id;
    private Long detailNo;
    private Long orderNo;
    private Long shopId;
    private String mchntCd;
    private Long goodsId;
    private String goodsName;
    private String goodsUnit;
    private String goodsBarCode;
    private Long goodsBasePrice;
    private Long goodsDisPrice;
    private Long goodsPrice;
    private BigDecimal goodsNumber;
    private BigDecimal refundGoodsNumber;
    private Long goodsPayAmt;
    private Long goodsRealPayAmt;
    private Long goodsTotalRefundAmt;
    private Long cashierDisPrice;
    private BigDecimal goodsCashierDiscount;
    private String disDiscountReason;
    private Integer goodsDisType;
    private Long goodsMemberPrice;
    private Long goodsMemberDisAmt;
    private Long goodsMemberPoints;
    private Long promotionId;
    private Integer goodsPromotionWay;
    private String unionpayCouponId;
    private Long avgPurchasePrice;
    private Integer goodsSalesCommission;
    private Integer goodsEmployeeCommission;
    private String specIdList;
    private String specDescList;
    private Integer isPackageGoods;
    private Long inPkgGoodsPrice;
    private String goodsBasket;
    private Long goodsLunchBoxFee;
    private Long prepackagedAmt;
    private Integer isThirdOrder;
    private String addDishChannel;
    private Integer dishState;
    private String dishUserMemo;
    private String dishCashierMemo;
    private String dishCancelReason;
    private String dishFuiouId;
    private Long dishUserId;
    private String dishCashierId;
    private Integer dishHasHurried;
    private Integer dishHasFinish;
    private Integer isDishConfirm;
    private Integer isOprByPad;
    private Integer isWeighGoods;
    private Integer markPrint;
    private Integer dishPrintState;
    private Integer kitchenPrint;
    private String printSerialNo;
    private Integer calcModel;
    private LocalDateTime itemCreateTime;
    private LocalDateTime dishUpdateTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
