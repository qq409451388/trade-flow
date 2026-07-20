package com.mtx.trade.common.id;

import com.baomidou.mybatisplus.core.incrementer.IdentifierGenerator;

/**
 * MyBatis-Plus {@link IdentifierGenerator} 适配器。
 *
 * <p>内部委托 {@link GlobalIdGenerator}，不重新创建雪花算法。
 * 实体默认使用 {@code @TableId(type = IdType.ASSIGN_ID)}。</p>
 *
 * <p>如果某个实体必须使用领域生成器，由业务层在插入前显式赋值：</p>
 * <pre>
 * entity.setId(idGeneratorRegistry.forDomain("order").nextId());
 * </pre>
 */
public class SnowflakeIdentifierGenerator implements IdentifierGenerator {

    private final GlobalIdGenerator globalIdGenerator;

    public SnowflakeIdentifierGenerator(GlobalIdGenerator globalIdGenerator) {
        if (globalIdGenerator == null) {
            throw new IllegalArgumentException("GlobalIdGenerator must not be null");
        }
        this.globalIdGenerator = globalIdGenerator;
    }

    @Override
    public Number nextId(Object entity) {
        return globalIdGenerator.nextId();
    }
}
