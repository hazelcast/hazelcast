package com.hazelcast.pipeline;

import com.hazelcast.core.ICompletableFuture;

public interface Pipeline {

    <E> ICompletableFuture<E> add(ICompletableFuture<E> f) throws InterruptedException;
}
