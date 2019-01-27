package com.hazelcast.pipeline;

import com.hazelcast.core.ICompletableFuture;

import java.util.List;

public interface Pipeline<E> {

    ICompletableFuture<E> add(ICompletableFuture<E> f) throws InterruptedException;

    List<E> results() throws Exception;
}
