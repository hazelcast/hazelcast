package com.hazelcast.core;

import java.util.concurrent.TimeUnit;

public interface AsyncSemaphore extends ISemaphore {

    CompletionFuture<Boolean> asyncInit(int permits);

    CompletionFuture<Void> asyncAcquire();

    CompletionFuture<Void> asyncAcquire(int permits);

    CompletionFuture<Integer> asyncAvailablePermits();

    CompletionFuture<Integer> asyncDrainPermits();

    CompletionFuture<Void> asyncReducePermits(int reduction);

    CompletionFuture<Void> asyncRelease();

    CompletionFuture<Void> asyncRelease(int permits);

    CompletionFuture<Boolean> asyncTryAcquire();

    CompletionFuture<Boolean> asyncTryAcquire(int permits);

    CompletionFuture<Boolean> asyncTryAcquire(long timeout, TimeUnit unit);

    CompletionFuture<Boolean> asyncTryAcquire(int permits, long timeout, TimeUnit unit);
}
