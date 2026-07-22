package com.mtx.trade.pipeline.config;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineShardingValueSupportTest {

    @Test
    void shouldExcludeOpenUpperBoundAtStartOfYear() {
        Range<LocalDateTime> range = Range.closedOpen(
                LocalDateTime.of(2026, 1, 1, 0, 0),
                LocalDateTime.of(2027, 1, 1, 0, 0));

        assertThat(PipelineShardingValueSupport.yearMatchesRange(2026, range)).isTrue();
        assertThat(PipelineShardingValueSupport.yearMatchesRange(2027, range)).isFalse();
    }

    @Test
    void shouldIncludeOpenUpperBoundYearWhenEndpointIsInsideYear() {
        Range<LocalDateTime> range = Range.lessThan(LocalDateTime.of(2027, 6, 1, 0, 0));

        assertThat(PipelineShardingValueSupport.yearMatchesRange(2027, range)).isTrue();
    }

    @Test
    void shouldIncludeClosedUpperBoundAtStartOfYear() {
        Range<LocalDateTime> range = Range.atMost(LocalDateTime.of(2027, 1, 1, 0, 0));

        assertThat(PipelineShardingValueSupport.yearMatchesRange(2027, range)).isTrue();
    }

    @Test
    void shouldExcludeYearsOutsideRange() {
        Range<LocalDateTime> range = Range.closed(
                LocalDateTime.of(2027, 2, 1, 0, 0),
                LocalDateTime.of(2027, 3, 1, 0, 0));

        assertThat(PipelineShardingValueSupport.yearMatchesRange(2026, range)).isFalse();
        assertThat(PipelineShardingValueSupport.yearMatchesRange(2028, range)).isFalse();
    }
}
