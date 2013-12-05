package com.hazelcast.core;

public interface AsyncAtomicLong extends IAtomicLong{

    CompletableFuture<Long> asyncAddAndGet(long delta);

    CompletableFuture<Boolean> asyncCompareAndSet(long expect, long update);

    CompletableFuture<Long> asyncDecrementAndGet();

    CompletableFuture<Long> asyncGet();

    CompletableFuture<Long> asyncGetAndAdd(long delta);

    CompletableFuture<Long> asyncGetAndSet(long newValue);

    CompletableFuture<Long> asyncIncrementAndGet();

    CompletableFuture<Long> asyncGetAndIncrement();

    CompletableFuture<Void> asyncSet(long newValue);

    CompletableFuture<Void> asyncAlter(Function<Long, Long> function);

    CompletableFuture<Long> asyncAlterAndGet(Function<Long, Long> function);

    CompletableFuture<Long> asyncGetAndAlter(Function<Long, Long> function);

    <R> CompletableFuture<R> asyncApply(Function<Long, R> function);
}
