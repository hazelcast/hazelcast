package com.hazelcast.core;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

public interface CompletableFuture<V> extends Future<V> {
    void andThen(ExecutionCallback<V> callback);
    void andThen(ExecutionCallback<V> callback, Executor executor);
}