package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.map.impl.querycache.QueryCacheScheduler;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Client side implementation of {@code QueryCacheScheduler}.
 *
 * @see QueryCacheScheduler
 */
public class ClientQueryCacheScheduler implements QueryCacheScheduler {

    private final ClientExecutionService executionService;

    public ClientQueryCacheScheduler(ClientExecutionService executionService) {
        this.executionService = executionService;
    }

    @Override
    public void execute(Runnable task) {
        executionService.execute(task);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRateWithDelaySeconds(Runnable task, long delaySeconds) {
        return executionService.scheduleWithFixedDelay(task, 1, delaySeconds, TimeUnit.SECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleWithDelaySeconds(Runnable task, long delaySeconds) {
        return executionService.schedule(task, delaySeconds, TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() {
        // intentionally not implemented for client-side.
    }
}
