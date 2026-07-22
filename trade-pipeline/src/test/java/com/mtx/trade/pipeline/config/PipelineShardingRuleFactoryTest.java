package com.mtx.trade.pipeline.config;

import org.apache.shardingsphere.single.config.SingleRuleConfiguration;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineShardingRuleFactoryTest {

    @Test
    void shouldRegisterPipelineEventLogsAsSingleTables() {
        SingleRuleConfiguration rule = PipelineShardingRuleFactory.createSingleTableRule();

        assertThat(rule.getTables()).containsExactlyInAnyOrder(
                "pipeline_mysql.pipeline_order_event_log",
                "pipeline_mysql.pipeline_payment_event_log",
                "pipeline_mysql.pipeline_event_pull_control");
        assertThat(rule.getDefaultDataSource()).contains("pipeline_mysql");
    }
}
