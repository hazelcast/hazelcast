package com.hazelcast.raft.impl.util;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.AbstractCompletableFuture;

import java.util.concurrent.Executor;

/**
 * TODO: Javadoc Pending...
 *
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
