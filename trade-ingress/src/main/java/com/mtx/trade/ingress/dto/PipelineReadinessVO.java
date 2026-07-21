package com.mtx.trade.ingress.dto;

import java.util.Map;

/** Pipeline事件consumer就绪状态。 */
public record PipelineReadinessVO(boolean ready, Map<String, Boolean> checks) {
}
