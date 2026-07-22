-- Ingress 熔断补充 Pipeline 不可用检测与 HALF_OPEN ACK 探测状态。
ALTER TABLE `trade_event_delivery_control`
  ADD COLUMN `pipeline_failure_count` INT UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'CLOSED状态Pipeline连续不可用次数' AFTER `health_success_count`,
  ADD COLUMN `probe_event_id` BIGINT NULL
    COMMENT 'HALF_OPEN等待ACK的探测event ID' AFTER `pipeline_failure_count`;
