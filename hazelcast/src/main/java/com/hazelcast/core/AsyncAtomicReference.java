package com.hazelcast.core;

public interface AsyncAtomicReference<E> extends IAtomicReference<E> {

    CompletableFuture<Boolean> asyncCompareAndSet(E expect, E update);

    CompletableFuture<E> asyncGet();

    CompletableFuture<Void> asyncSet(E newValue);

    CompletableFuture<E> asyncGetAndSet(E newValue);

    CompletableFuture<E> asyncSetAndGet(E update);

    CompletableFuture<Boolean> asyncIsNull();

    CompletableFuture<Void> asyncClear();

    CompletableFuture<Boolean> asyncContains(E value);

    CompletableFuture<Void> asyncAlter(Function<E, E> function);

    CompletableFuture<E> asyncAlterAndGet(Function<E, E> function);

    CompletableFuture<E> asyncGetAndAlter(Function<E, E> function);

    <R> CompletableFuture<R> asyncApply(Function<E, R> function);
}
