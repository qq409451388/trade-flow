package com.mtx.trade.common.id.exception;

/**
 * ID 生成相关异常的基类。
 */
public class IdGenerationException extends RuntimeException {

    public IdGenerationException(String message) {
        super(message);
    }

    public IdGenerationException(String message, Throwable cause) {
        super(message, cause);
    }
}
