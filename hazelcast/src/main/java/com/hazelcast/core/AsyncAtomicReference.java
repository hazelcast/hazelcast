package com.hazelcast.core;

import com.hazelcast.spi.annotation.Beta;

/**
 * A {@link IAtomicReference} that exposes its operations using a {@link ICompletableFuture}
 * so it can be used in the reactive programming model approach.
 *
 * @since 3.2
 */
@Beta
public interface AsyncAtomicReference<E> extends IAtomicReference<E> {

    ICompletableFuture<Boolean> asyncCompareAndSet(E expect, E update);

    ICompletableFuture<E> asyncGet();

    ICompletableFuture<Void> asyncSet(E newValue);

    ICompletableFuture<E> asyncGetAndSet(E newValue);

    ICompletableFuture<E> asyncSetAndGet(E update);

    ICompletableFuture<Boolean> asyncIsNull();

    ICompletableFuture<Void> asyncClear();

    ICompletableFuture<Boolean> asyncContains(E value);

    ICompletableFuture<Void> asyncAlter(IFunction<E, E> function);

    ICompletableFuture<E> asyncAlterAndGet(IFunction<E, E> function);

    ICompletableFuture<E> asyncGetAndAlter(IFunction<E, E> function);

    <R> ICompletableFuture<R> asyncApply(IFunction<E, R> function);
}
