package com.mtx.trade.pipeline.event.consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionedEventExecutorTest {

    private PartitionedEventExecutor executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Test
    void shouldKeepSubmissionOrderForSameBusinessKey() {
        executor = new PartitionedEventExecutor(4, 10, "test-event-worker-");
        List<Integer> executionOrder = Collections.synchronizedList(new ArrayList<>());

        CompletableFuture<Void> first = executor.submit("1:order-100", () -> executionOrder.add(1));
        CompletableFuture<Void> second = executor.submit("1:order-100", () -> executionOrder.add(2));
        CompletableFuture<Void> third = executor.submit("1:order-100", () -> executionOrder.add(3));
        CompletableFuture.allOf(first, second, third).join();

        assertThat(executionOrder).containsExactly(1, 2, 3);
        assertThat(executor.snapshot().completedTasks()).isEqualTo(3);
    }

    @Test
    void shouldRunDifferentPartitionsConcurrently() throws Exception {
        executor = new PartitionedEventExecutor(2, 10, "test-event-worker-");
        CountDownLatch bothStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        Runnable blockingTask = () -> {
            bothStarted.countDown();
            try {
                release.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        CompletableFuture<Void> first = executor.submit("a", blockingTask);
        CompletableFuture<Void> second = executor.submit("b", blockingTask);

        assertThat(bothStarted.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.snapshot().activeWorkers()).isEqualTo(2);
        release.countDown();
        CompletableFuture.allOf(first, second).join();
    }
}
