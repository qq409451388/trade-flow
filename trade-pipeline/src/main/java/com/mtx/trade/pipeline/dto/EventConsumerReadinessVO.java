package com.mtx.trade.pipeline.dto;

import java.util.Map;

/** Ingress恢复Redis投递前使用的Pipeline消费就绪状态。 */
public record EventConsumerReadinessVO(boolean ready, Map<String, Boolean> checks) {
}
