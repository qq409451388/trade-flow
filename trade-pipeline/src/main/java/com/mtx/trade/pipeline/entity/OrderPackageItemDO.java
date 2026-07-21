package com.mtx.trade.pipeline.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/** 订单套餐子商品明细。 */
@Data
@TableName("oms_order_package_item")
public class OrderPackageItemDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.INPUT)
    private Long id;
    private Long packageDetailNo;
    private Long relatePkgDetailNo;
    private Long orderNo;
    private Long goodsId;
    private Long shopId;
    private String mchntCd;
    private String goodsName;
    private String goodsUnit;
    private String goodsBarCode;
    private Long goodsBasePrice;
    private Long goodsDisPrice;
    private Long goodsMemberPrice;
    private Long goodsMemberDisAmt;
    private BigDecimal goodsCashierDiscount;
    private Long cashierDisPrice;
    private Long goodsPrice;
    private Long goodsPayAmt;
    private BigDecimal goodsNumber;
    private BigDecimal packageNumber;
    private BigDecimal pkgGoodsCopies;
    private Long pkgGoodsExtraPrice;
    private Long avgPurchasePrice;
    private Long goodsLunchBoxFee;
    private Long goodsMemberPoints;
    private Integer goodsIndex;
    private Integer markPrint;
    private Integer isWeighGoods;
    private String specIdList;
    private String specDescList;
    private String thirdDetailNo;
    private String packageDishMemo;
    private LocalDateTime itemCreateTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
