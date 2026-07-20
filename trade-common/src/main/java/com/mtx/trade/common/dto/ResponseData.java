package com.mtx.trade.common.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import com.mtx.trade.common.enums.ErrorCode;

import static com.mtx.trade.common.enums.ErrorCode.SUCCESS;

/**
 * 统一返回格式。
 *
 * <p>所有接口返回的最外层必须是 {@code ResponseData<XxxVO>}。</p>
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ResponseData<T> {

    private Integer code;
    private T data;
    private String message;

    public static <T> ResponseData<T> success(T data) {
        return success(data, SUCCESS.getMessage());
    }

    public static <T> ResponseData<T> success(T data, String message) {
        return new ResponseData<T>(SUCCESS.getCode(), data, message);
    }

    public static <T> ResponseData<T> success() {
        return success(null, SUCCESS.getMessage());
    }

    public static <T> ResponseData<T> fail(int code, String message, T data) {
        return new ResponseData<T>(code, data, message);
    }

    public static <T> ResponseData<T> fail(ErrorCode errorCode) {
        return new ResponseData<T>(errorCode.getCode(), null, errorCode.getMessage());
    }

    public static <T> ResponseData<T> fail(ErrorCode errorCode, String customMsg) {
        return new ResponseData<T>(errorCode.getCode(), null, customMsg);
    }
}
