package com.mtx.trade.pipeline.service;

import com.mtx.trade.common.enums.ErrorCode;
import com.mtx.trade.common.exception.BusinessException;
import com.mtx.trade.storage.api.StorageMetadata;

/** Pipeline 读取 Storage 原文后的完整性校验。 */
final class StorageContentValidator {

    private StorageContentValidator() {
    }

    static void requireComplete(StorageMetadata metadata, byte[] content, String eventName) {
        if (metadata.payloadLength() != content.length) {
            throw new BusinessException(
                    ErrorCode.PARAM_INVALID,
                    eventName + "事件关联的 Storage 原文长度不一致，expected="
                            + metadata.payloadLength() + ", actual=" + content.length);
        }
    }
}
