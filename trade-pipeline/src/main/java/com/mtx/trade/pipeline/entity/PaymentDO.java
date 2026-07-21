package com.mtx.trade.pipeline.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** 富友支付或退款流水。 */
@Data
@TableName("oms_payment")
public class PaymentDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.INPUT)
    private Long id;
    private String paySsn;
    private String sourcePaySsn;
    private Long orderNo;
    private String mchntCd;
    private Long shopId;
    private String shopName;
    private LocalDateTime payTime;
    private LocalDateTime refundTime;
    private String payType;
    private String payName;
    private Integer payState;
    private Long payAmt;
    private Long feeAmt;
    private Long refundAmt;
    private Long balanceDisAmt;
    private Long faceAmt;
    private Integer fySettle;
    private String thirdOrderNo;
    private String channelTradeNo;
    private String bigCategory;
    private String smallCategory;
    private String orderSource;
    private String openId;
    private String orderType;
    private String channelType;
    private String memberName;
    private String phone;
    private String memberCardNo;
    private String memberLevel;
    private Long storageId;
    private byte[] payloadSha256;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
