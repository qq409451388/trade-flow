package com.mtx.trade.pipeline.task;

import com.mtx.trade.common.enums.ContentType;
import com.mtx.trade.common.utils.EnterpriseWechatRobotUtils;
import com.mtx.trade.pipeline.config.EventConsumerConfiguration;
import com.mtx.trade.pipeline.config.ExhaustedEventPullProperties;
import com.mtx.trade.pipeline.dto.OrderEventPullCommand;
import com.mtx.trade.pipeline.dto.OrderEventPullResult;
import com.mtx.trade.pipeline.dto.PaymentEventPullCommand;
import com.mtx.trade.pipeline.dto.PaymentEventPullResult;
import com.mtx.trade.pipeline.service.EventPullLeaseService;
import com.mtx.trade.pipeline.service.OrderEventPullService;
import com.mtx.trade.pipeline.service.PaymentEventPullService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.LongFunction;

/** 定时主动拉取 Ingress 已耗尽事件，作为 Redis Stream 投递链路的最终兜底。 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "trade.pipeline.exhausted-event-pull",
        name = "enabled", havingValue = "true", matchIfMissing = true)
public class ExhaustedEventPullScheduler {

    private static final int MAX_BATCH_SIZE = 500;

    private final ExhaustedEventPullProperties properties;
    private final EventPullLeaseService leaseService;
    private final OrderEventPullService orderEventPullService;
    private final PaymentEventPullService paymentEventPullService;
    private final EnterpriseWechatRobotUtils enterpriseWechatRobotUtils;

    @Scheduled(
            initialDelayString = "${trade.pipeline.exhausted-event-pull.initial-delay-ms:60000}",
            fixedDelayString = "${trade.pipeline.exhausted-event-pull.fixed-delay-ms:60000}",
            scheduler = EventConsumerConfiguration.EXHAUSTED_PULL_SCHEDULER)
    public void pullOrders() {
        int contentType = ContentType.ORDER.getCode();
        if (!acquire(contentType)) {
            log.debug("[Exhausted Event Pull] 🔄 Another Pipeline instance owns the order pull lease; "
                    + "this run was skipped.");
            return;
        }
        try {
            drain("order", contentType,
                    cursor -> orderEventPullService.pull(
                            new OrderEventPullCommand(List.of(), batchSize(), cursor)),
                    OrderEventPullResult::eventId,
                    OrderEventPullResult::status,
                    OrderEventPullResult::message);
        } catch (Exception e) {
            log.error("[Exhausted Event Pull] ❌ Order pull batch failed; the lease cursor remains recoverable.", e);
        } finally {
            release(contentType);
        }
    }

    @Scheduled(
            initialDelayString = "${trade.pipeline.exhausted-event-pull.initial-delay-ms:60000}",
            fixedDelayString = "${trade.pipeline.exhausted-event-pull.fixed-delay-ms:60000}",
            scheduler = EventConsumerConfiguration.EXHAUSTED_PULL_SCHEDULER)
    public void pullPayments() {
        int contentType = ContentType.PAYMENT.getCode();
        if (!acquire(contentType)) {
            log.debug("[Exhausted Event Pull] 🔄 Another Pipeline instance owns the payment pull lease; "
                    + "this run was skipped.");
            return;
        }
        try {
            drain("payment", contentType,
                    cursor -> paymentEventPullService.pull(
                            new PaymentEventPullCommand(List.of(), batchSize(), cursor)),
                    PaymentEventPullResult::eventId,
                    PaymentEventPullResult::status,
                    PaymentEventPullResult::message);
        } catch (Exception e) {
            log.error("[Exhausted Event Pull] ❌ Payment pull batch failed; the lease cursor remains recoverable.", e);
        } finally {
            release(contentType);
        }
    }

    private int batchSize() {
        int batchSize = properties.getBatchSize();
        if (batchSize <= 0 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("trade.pipeline.exhausted-event-pull.batch-size 必须为1~500");
        }
        return batchSize;
    }

    private <T> void drain(
            String type,
            int contentType,
            LongFunction<List<T>> pullBatch,
            Function<T, Long> eventId,
            Function<T, String> status,
            Function<T, String> message) {
        int batchSize = batchSize();
        int maxBatches = maxBatchesPerRun();
        long deadlineNanos = System.nanoTime() + maxRunDuration().toNanos();
        long cursor = 0L;
        long total = 0L;
        long failed = 0L;

        log.debug("[Exhausted Event Pull] 🔄 Backlog drain started. type={}, batchSize={}, "
                        + "maxBatches={}, parallelism={}",
                type, batchSize, maxBatches, properties.getParallelism());
        for (int batch = 1; batch <= maxBatches; batch++) {
            List<T> results = pullBatch.apply(cursor);
            if (results.isEmpty()) {
                log.debug("[Exhausted Event Pull] ✅ Backlog drain completed. "
                                + "type={}, batches={}, total={}, succeeded={}, failed={}",
                        type, batch - 1, total, total - failed, failed);
                return;
            }

            long batchFailed = results.stream()
                    .map(status)
                    .filter(ExhaustedEventPullScheduler::isFailed)
                    .count();
            long nextCursor = results.stream()
                    .map(eventId)
                    .filter(java.util.Objects::nonNull)
                    .mapToLong(Long::longValue)
                    .max()
                    .orElse(cursor);
            total += results.size();
            failed += batchFailed;
            notifyTerminalFailures(type, batch, results, eventId, status, message);
            log.debug("[Exhausted Event Pull] 🔄 Backlog batch completed. "
                            + "type={}, batch={}, batchTotal={}, batchFailed={}, cursor={}, total={}",
                    type, batch, results.size(), batchFailed, nextCursor, total);

            if (results.size() < batchSize) {
                logCompletion(type, batch, total, failed);
                return;
            }
            if (nextCursor <= cursor) {
                log.error("[Exhausted Event Pull] ❌ Backlog cursor did not advance; this run was stopped "
                                + "to prevent a hot loop. type={}, cursor={}, batch={}",
                        type, cursor, batch);
                return;
            }
            cursor = nextCursor;
            if (System.nanoTime() >= deadlineNanos) {
                log.warn("[Exhausted Event Pull] 🔄 Run duration limit reached; remaining backlog will "
                                + "continue next run. type={}, batches={}, total={}, cursor={}",
                        type, batch, total, cursor);
                return;
            }
            if (!leaseService.renew(contentType)) {
                log.warn("[Exhausted Event Pull] 🔄 Pull lease could not be renewed; this run stopped before "
                                + "loading another batch. type={}, batches={}, total={}, cursor={}",
                        type, batch, total, cursor);
                return;
            }
        }
        log.warn("[Exhausted Event Pull] 🔄 Batch limit reached; remaining backlog will continue next run. "
                        + "type={}, maxBatches={}, total={}, cursor={}",
                type, maxBatches, total, cursor);
    }

    private int maxBatchesPerRun() {
        int value = properties.getMaxBatchesPerRun();
        if (value <= 0 || value > 1000) {
            throw new IllegalArgumentException("trade.pipeline.exhausted-event-pull.max-batches-per-run 必须为1~1000");
        }
        return value;
    }

    private Duration maxRunDuration() {
        Duration value = properties.getMaxRunDuration();
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("trade.pipeline.exhausted-event-pull.max-run-duration 必须为正数");
        }
        return value;
    }

    private boolean acquire(int contentType) {
        try {
            return leaseService.tryAcquire(contentType);
        } catch (Exception e) {
            log.error("[Exhausted Event Pull] ❌ Pull lease acquisition failed; this run was skipped. "
                    + "contentType={}", contentType, e);
            return false;
        }
    }

    private void release(int contentType) {
        try {
            leaseService.release(contentType);
        } catch (Exception e) {
            log.warn("[Exhausted Event Pull] 🔄 Pull lease release failed; lease expiry will release it. "
                    + "contentType={}", contentType, e);
        }
    }

    private static void logCompletion(String type, int batches, long total, long failed) {
        if (failed == 0) {
            log.info("[Exhausted Event Pull] ✅ Backlog drain completed. "
                            + "type={}, batches={}, total={}, succeeded={}, failed=0",
                    type, batches, total, total);
        } else {
            log.error("[Exhausted Event Pull] ❌ Backlog drain completed with failures; failed events remain "
                            + "eligible for the next sweep. type={}, batches={}, total={}, succeeded={}, failed={}",
                    type, batches, total, total - failed, failed);
        }
    }

    private static boolean isFailed(String status) {
        return status == null || status.equals("FAILED") || status.endsWith("_FAILED");
    }

    private <T> void notifyTerminalFailures(
            String type,
            int batch,
            List<T> results,
            Function<T, Long> eventId,
            Function<T, String> status,
            Function<T, String> message) {
        List<T> terminalFailures = results.stream()
                .filter(result -> {
                    String value = status.apply(result);
                    return value != null && value.startsWith("TERMINAL_");
                })
                .toList();
        if (terminalFailures.isEmpty()) {
            return;
        }
        String details = terminalFailures.stream()
                .limit(10)
                .map(result -> "eventId=" + eventId.apply(result) + ", "
                        + abbreviate(message.apply(result), 240))
                .collect(java.util.stream.Collectors.joining("\n"));
        enterpriseWechatRobotUtils.sendTextQuietly(
                "[Trade Pipeline] Exhausted event terminal failures\n"
                        + "type=" + type + ", batch=" + batch
                        + ", count=" + terminalFailures.size() + "\n"
                        + details);
    }

    private static String abbreviate(String value, int maxLength) {
        if (value == null) {
            return "unknown";
        }
        String singleLine = value.replace('\n', ' ').replace('\r', ' ');
        return singleLine.length() <= maxLength
                ? singleLine : singleLine.substring(0, maxLength) + "...";
    }
}
