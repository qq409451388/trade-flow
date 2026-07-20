package com.mtx.trade.common.id;

/**
 * 时间提供者抽象，用于解耦时间获取，便于测试。
 *
 * <p>生产环境使用 {@link SystemTimeProvider}（基于 {@link System#currentTimeMillis()}），
 * 测试时可替换为可控实现以模拟时钟回拨等场景。</p>
 */
@FunctionalInterface
public interface TimeProvider {

    /**
     * 返回当前时间的毫秒数（与 {@link System#currentTimeMillis()} 语义一致）。
     *
     * @return 当前时间毫秒数
     */
    long currentTimeMillis();
}
