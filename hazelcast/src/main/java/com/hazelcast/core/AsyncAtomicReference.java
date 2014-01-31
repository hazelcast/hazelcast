package com.hazelcast.core;

/**
 * A {@link IAtomicReference} that exposes its operations using a {@link ICompletableFuture}
 * so it can be used in the reactive programming model approach.
 */
public interface AsyncAtomicReference<E> extends IAtomicReference<E> {

    ICompletableFuture<Boolean> asyncCompareAndSet(E expect, E update);

    ICompletableFuture<E> asyncGet();

    ICompletableFuture<Void> asyncSet(E newValue);

    ICompletableFuture<E> asyncGetAndSet(E newValue);

    ICompletableFuture<E> asyncSetAndGet(E update);

    ICompletableFuture<Boolean> asyncIsNull();

    ICompletableFuture<Void> asyncClear();

    ICompletableFuture<Boolean> asyncContains(E value);

    ICompletableFuture<Void> asyncAlter(Function<E, E> function);

    ICompletableFuture<E> asyncAlterAndGet(Function<E, E> function);

    ICompletableFuture<E> asyncGetAndAlter(Function<E, E> function);

    <R> ICompletableFuture<R> asyncApply(Function<E, R> function);
}
