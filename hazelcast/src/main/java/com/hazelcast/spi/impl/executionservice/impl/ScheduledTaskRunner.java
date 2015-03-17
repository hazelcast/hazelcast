package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Executor;

class ScheduledTaskRunner implements Runnable {

    private final Executor executor;
    private final Runnable runnable;

    public ScheduledTaskRunner(Runnable runnable, Executor executor) {
        this.executor = executor;
        this.runnable = runnable;
    }

    @Override
    public void run() {
        try {
            executor.execute(runnable);
        } catch (Throwable t) {
            ExceptionUtil.sneakyThrow(t);
        }
    }
}
