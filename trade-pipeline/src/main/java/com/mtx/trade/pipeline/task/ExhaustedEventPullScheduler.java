package com.mtx.trade.pipeline.task;

import com.mtx.trade.common.enums.ContentType;
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
            log.info("[Exhausted Event Pull] 🔄 Pulling exhausted order events. batchSize={}", batchSize());
            List<OrderEventPullResult> results = orderEventPullService.pull(
                    new OrderEventPullCommand(List.of(), batchSize()));
            logResults("order", results.stream().map(OrderEventPullResult::status).toList());
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
            log.info("[Exhausted Event Pull] 🔄 Pulling exhausted payment events. batchSize={}", batchSize());
            List<PaymentEventPullResult> results = paymentEventPullService.pull(
                    new PaymentEventPullCommand(List.of(), batchSize()));
            logResults("payment", results.stream().map(PaymentEventPullResult::status).toList());
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

    private static void logResults(String type, List<String> statuses) {
        if (statuses.isEmpty()) {
            log.info("[Exhausted Event Pull] ✅ No exhausted events are waiting. type={}", type);
            return;
        }
        long failed = statuses.stream().filter(ExhaustedEventPullScheduler::isFailed).count();
        if (failed == 0) {
            log.info("[Exhausted Event Pull] ✅ Pull batch completed. type={}, total={}, succeeded={}, failed=0",
                    type, statuses.size(), statuses.size());
        } else {
            log.error("[Exhausted Event Pull] ❌ Pull batch completed with failures. "
                            + "type={}, total={}, succeeded={}, failed={}",
                    type, statuses.size(), statuses.size() - failed, failed);
        }
    }

    private static boolean isFailed(String status) {
        return status == null || status.equals("FAILED") || status.endsWith("_FAILED");
    }
}
