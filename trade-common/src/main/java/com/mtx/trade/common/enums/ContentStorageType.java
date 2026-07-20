package com.mtx.trade.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 内容存储类型（trade_storage.content_storage_type）。
 */
@Getter
@AllArgsConstructor
public enum ContentStorageType {

    BLOB(1, "BLOB"),
    LOCAL_ARCHIVE(2, "本地归档"),
    OSS_ARCHIVE(3, "OSS归档");

    private final int code;
    private final String desc;
}
