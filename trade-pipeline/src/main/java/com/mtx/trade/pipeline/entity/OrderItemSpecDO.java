package com.mtx.trade.pipeline.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** 订单商品规格明细。 */
@Data
@TableName("oms_order_item_spec")
public class OrderItemSpecDO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(type = IdType.INPUT)
    private Long id;
    private Long orderSpecId;
    private Long detailNo;
    private Long specId;
    private String specName;
    private Long specDetailId;
    private String specDetailDesc;
    private Long detailExtraPrice;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
