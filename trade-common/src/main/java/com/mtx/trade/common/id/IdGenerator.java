package com.mtx.trade.common.id;

/**
 * 通用 ID 生成器接口。
 *
 * <p>所有具体生成器（全局、领域）均实现此接口。</p>
 */
public interface IdGenerator {

    /**
     * 生成下一个全局唯一 ID。
     *
     * @return 正数 {@code long} 类型 ID
     */
    long nextId();

    /**
     * 生成下一个全局唯一 ID 的字符串表示。
     *
     * @return ID 字符串
     */
    default String nextIdString() {
        return Long.toString(nextId());
    }
}
