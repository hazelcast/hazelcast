package com.hazelcast.client.spi.impl;

import com.hazelcast.client.spi.ClientExecutionService;

import java.util.concurrent.*;

/**
 * @mdogan 5/16/13
 */
public final class ClientExecutionServiceImpl implements ClientExecutionService {

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return executor.submit(task);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return executor.submit(task);
    }

    @Override
    public ScheduledFuture<?> schedule(final Runnable command, long delay, TimeUnit unit) {
        return scheduledExecutor.schedule(new Runnable() {
            public void run() {
                execute(command);
            }
        }, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                execute(command);
            }
        }, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                execute(command);
            }
        }, initialDelay, period, unit);
    }

    public void shutdown() {
        scheduledExecutor.shutdownNow();
        executor.shutdownNow();
    }
}
