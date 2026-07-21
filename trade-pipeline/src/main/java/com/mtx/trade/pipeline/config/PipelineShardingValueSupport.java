package com.mtx.trade.pipeline.config;

import com.google.common.collect.Range;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/** Pipeline 分片算法共用的时间值转换。 */
final class PipelineShardingValueSupport {

    private PipelineShardingValueSupport() {
    }

    static int yearOf(Object value) {
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime.getYear();
        }
        if (value instanceof LocalDate localDate) {
            return localDate.getYear();
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toLocalDateTime().getYear();
        }
        if (value instanceof java.sql.Date date) {
            return date.toLocalDate().getYear();
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.getYear();
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return zonedDateTime.getYear();
        }
        if (value instanceof Instant instant) {
            return instant.atZone(ZoneId.systemDefault()).getYear();
        }
        if (value instanceof java.util.Date date) {
            return date.toInstant().atZone(ZoneId.systemDefault()).getYear();
        }
        if (value instanceof Number number) {
            int year = number.intValue();
            if (year >= 2000 && year <= 9999) {
                return year;
            }
        }
        if (value instanceof CharSequence text && text.length() >= 4) {
            try {
                return Integer.parseInt(text.subSequence(0, 4).toString());
            } catch (NumberFormatException ignored) {
                // 统一走下面的明确错误。
            }
        }
        throw new IllegalArgumentException("Unsupported pipeline year sharding value: " + value);
    }

    static Set<Integer> yearsOf(Collection<? extends Comparable<?>> values) {
        Set<Integer> result = new LinkedHashSet<>();
        if (values != null) {
            values.forEach(value -> result.add(yearOf(value)));
        }
        return result;
    }

    static boolean yearMatchesRange(int year, Range<? extends Comparable<?>> range) {
        if (range == null) {
            return true;
        }
        if (range.hasLowerBound() && year < yearOf(range.lowerEndpoint())) {
            return false;
        }
        return !range.hasUpperBound() || year <= yearOf(range.upperEndpoint());
    }

    static String requireSingleTarget(Collection<String> availableTargets, String expectedSuffix) {
        return availableTargets.stream()
                .filter(target -> target.endsWith(expectedSuffix))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "No pipeline physical table configured for suffix " + expectedSuffix));
    }
}
