package com.mtx.trade.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 业务错误码。
 *
 * <p>用于 {@link com.mtx.trade.common.dto.ResponseData#fail(ErrorCode)}
 * 与 {@link com.mtx.trade.common.exception.BusinessException}。</p>
 */
@Getter
@AllArgsConstructor
public enum ErrorCode {

    SUCCESS(0, "success"),

    SYSTEM_ERROR(500, "系统异常"),
    PARAM_INVALID(400, "参数校验失败"),
    UNAUTHORIZED(401, "未登录或登录已过期"),
    FORBIDDEN(403, "无权限访问"),
    NOT_FOUND(404, "资源不存在"),
    BUSINESS_ERROR(1000, "业务异常"),
    DATA_CREATE_ERROR(1001, "数据操作异常")
    ;

    private final int code;
    private final String message;
}
