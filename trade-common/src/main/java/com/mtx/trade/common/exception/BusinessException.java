package com.mtx.trade.common.exception;

import com.mtx.trade.common.enums.ErrorCode;
import lombok.Getter;

/**
 * 业务异常。
 *
 * <p>接口业务逻辑中需要使用异常的场景，建议使用 {@code BusinessException}，
 * 由全局异常处理器捕获后以标准 {@link com.mtx.trade.common.dto.ResponseData} 格式返回。</p>
 */
@Getter
public class BusinessException extends RuntimeException {

    private final int code;

    public BusinessException(ErrorCode errorCode) {
        super(errorCode.getMessage());
        this.code = errorCode.getCode();
    }

    public BusinessException(ErrorCode errorCode, String customMessage) {
        super(customMessage);
        this.code = errorCode.getCode();
    }

    public BusinessException(int code, String message) {
        super(message);
        this.code = code;
    }
}
