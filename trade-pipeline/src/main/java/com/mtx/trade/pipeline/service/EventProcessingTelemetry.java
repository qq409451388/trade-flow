package com.mtx.trade.pipeline.service;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/** 实时消费结果的进程内累计计数；重启后清零，由性能窗口计算区间增量。 */
@Component
public class EventProcessingTelemetry {

    public static final String ORDER = "order";
    public static final String PAYMENT = "payment";

    private final Map<String, Counters> channels = new ConcurrentHashMap<>();

    public void markSuccess(String channel) {
        counters(channel).success.increment();
    }

    public void markFailure(String channel) {
        counters(channel).failure.increment();
    }

    public void markIngressAckFailure(String channel) {
        counters(channel).ingressAckFailure.increment();
    }

    public void markRedisXackFailure(String channel) {
        counters(channel).redisXackFailure.increment();
    }

    public Snapshot snapshot(String channel) {
        Counters counters = counters(channel);
        return new Snapshot(
                counters.success.sum(),
                counters.failure.sum(),
                counters.ingressAckFailure.sum(),
                counters.redisXackFailure.sum());
    }

    private Counters counters(String channel) {
        return channels.computeIfAbsent(channel, ignored -> new Counters());
    }

    private static final class Counters {
        private final LongAdder success = new LongAdder();
        private final LongAdder failure = new LongAdder();
        private final LongAdder ingressAckFailure = new LongAdder();
        private final LongAdder redisXackFailure = new LongAdder();
    }

    public record Snapshot(
            long successfulEvents,
            long failedEvents,
            long ingressAckFailures,
            long redisXackFailures) {
    }
}
