package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.Future;

final class CompletableFutureEntry<V> {

    final BasicCompletableFuture<V> completableFuture;

    CompletableFutureEntry(Future<V> future, NodeEngine nodeEngine) {
        this.completableFuture = new BasicCompletableFuture<V>(future, nodeEngine);
    }

    boolean processState() {
        if (completableFuture.isDone()) {
            Object result;
            try {
                result = completableFuture.future.get();
            } catch (Throwable t) {
                result = t;
            }
            completableFuture.setResult(result);
            return true;
        }
        return false;
    }
}
