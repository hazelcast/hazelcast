package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.AbstractCompletableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class BasicCompletableFuture<V> extends AbstractCompletableFuture<V> {

    final Future<V> future;

    BasicCompletableFuture(Future<V> future, NodeEngine nodeEngine) {
        super(nodeEngine, nodeEngine.getLogger(BasicCompletableFuture.class));
        this.future = future;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        boolean done = future.isDone();
        if (done && !super.isDone()) {
            forceSetResult();
            return true;
        }
        return done || super.isDone();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        V result = future.get(timeout, unit);
        // If not yet set by CompletableFuture task runner, we can go for it!
        if (!super.isDone()) {
            setResult(result);
        }
        return result;
    }

    private void forceSetResult() {
        Object result;
        try {
            result = future.get();
        } catch (Throwable t) {
            result = t;
        }
        setResult(result);
    }
}
