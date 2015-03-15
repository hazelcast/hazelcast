package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;

import java.util.concurrent.TimeUnit;

public final class LocalEventDispatcher implements StripedRunnable, TimeoutRunnable {
    private EventServiceImpl eventService;
    private final String serviceName;
    private final Object event;
    private final Object listener;
    private final int orderKey;
    private final long timeoutMs;

    LocalEventDispatcher(EventServiceImpl eventService, String serviceName, Object event, Object listener,
                         int orderKey, long timeoutMs) {
        this.eventService = eventService;
        this.serviceName = serviceName;
        this.event = event;
        this.listener = listener;
        this.orderKey = orderKey;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public long getTimeout() {
        return timeoutMs;
    }

    @Override
    public TimeUnit getTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    @Override
    public void run() {
        final EventPublishingService<Object, Object> service = eventService.nodeEngine.getService(serviceName);
        if (service == null) {
            if (eventService.nodeEngine.isActive()) {
                throw new IllegalArgumentException("Service[" + serviceName + "] could not be found!");
            }
            return;
        }

        service.dispatchEvent(event, listener);
    }

    @Override
    public int getKey() {
        return orderKey;
    }
}
