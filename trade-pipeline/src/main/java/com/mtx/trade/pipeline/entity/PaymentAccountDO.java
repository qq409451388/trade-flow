package com.mtx.trade.pipeline.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** 支付结算账户明细。 */
@Data
@TableName("oms_payment_account")
public class PaymentAccountDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.INPUT)
    private Long id;
    private Long paymentId;
    private String paySsn;
    private Integer accountSeq;
    private String custAcntTp;
    private String mchntCd;
    private Long shopId;
    private String outAcntNm;
    private String outAcntNo;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
