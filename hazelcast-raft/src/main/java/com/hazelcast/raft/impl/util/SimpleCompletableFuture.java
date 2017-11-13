package com.hazelcast.raft.impl.util;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.AbstractCompletableFuture;

import java.util.concurrent.Executor;

/**
 * A simple {@link AbstractCompletableFuture} implementation that allows completing
 * via public {@link #setResult(Object)} method.
 *
 * @param <T> The result type returned by this future
 */
public class SimpleCompletableFuture<T> extends AbstractCompletableFuture<T> {

    public SimpleCompletableFuture(Executor executor, ILogger logger) {
        super(executor, logger);
    }

    @Override
    public void setResult(Object result) {
        super.setResult(result);
    }
}
