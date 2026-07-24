package com.mtx.trade.pipeline.event.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** 保存实时 Stream 订阅句柄，并在监听任务意外退出后重新注册。 */
@Slf4j
@Service
public class EventStreamListenerRegistry {

    private final Map<Integer, ListenerState> listeners = new ConcurrentHashMap<>();

    public void bind(
            int contentType,
            String type,
            StreamMessageListenerContainer<?, ?> container,
            Subscription subscription,
            Supplier<Subscription> subscriptionFactory) {
        listeners.put(contentType, new ListenerState(type, container, subscription, subscriptionFactory));
    }

    public boolean isReady(int contentType) {
        ListenerState state = listeners.get(contentType);
        return state != null && state.isReady();
    }

    public void ensureActive(int contentType) {
        ListenerState state = listeners.get(contentType);
        if (state == null) {
            return;
        }
        state.ensureActive();
    }

    private static final class ListenerState {
        private final String type;
        private final StreamMessageListenerContainer<?, ?> container;
        private final Supplier<Subscription> subscriptionFactory;
        private volatile Subscription subscription;

        private ListenerState(
                String type,
                StreamMessageListenerContainer<?, ?> container,
                Subscription subscription,
                Supplier<Subscription> subscriptionFactory) {
            this.type = type;
            this.container = container;
            this.subscription = subscription;
            this.subscriptionFactory = subscriptionFactory;
        }

        private boolean isReady() {
            Subscription current = subscription;
            return container.isRunning() && current != null && current.isActive();
        }

        private synchronized void ensureActive() {
            if (isReady()) {
                return;
            }
            try {
                if (!container.isRunning()) {
                    log.warn("[Stream Watchdog] 🔄 {} listener container stopped; restarting it.", type);
                    container.start();
                }
                Subscription current = subscription;
                if (current == null || !current.isActive()) {
                    subscription = subscriptionFactory.get();
                    log.warn("[Stream Watchdog] 🔄 {} Stream subscription was inactive; a new subscription "
                            + "has been registered.", type);
                }
                if (isReady()) {
                    log.info("[Stream Watchdog] ✅ {} realtime Stream listener is active.", type);
                }
            } catch (Exception e) {
                log.error("[Stream Watchdog] ❌ {} realtime Stream listener recovery failed; the watchdog will retry.",
                        type, e);
            }
        }
    }
}
