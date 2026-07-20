package com.mtx.trade.common.id;

/**
 * 全局 ID 生成器。
 *
 * <p>真正的全局唯一 ID 生成入口，所有领域共享同一个底层雪花核心。</p>
 */
public interface GlobalIdGenerator extends IdGenerator {
}
