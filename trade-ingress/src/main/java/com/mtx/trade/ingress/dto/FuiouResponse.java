package com.mtx.trade.ingress.dto;

import lombok.Data;

/**
 * 富友要求的响应报文
 *
 * resultCode : 000000=成功，其他=失败（富友会重试）
 * resultMsg  : 响应描述
 */
@Data
public class FuiouResponse {

    private String resultCode;
    private String resultMsg;

    public FuiouResponse() {
    }

    public FuiouResponse(String resultCode, String resultMsg) {
        this.resultCode = resultCode;
        this.resultMsg = resultMsg;
    }

    public static FuiouResponse ok() {
        return new FuiouResponse("000000", "成功");
    }

    public static FuiouResponse fail(String msg) {
        return new FuiouResponse("999999", msg);
    }
}
