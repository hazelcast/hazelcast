package com.hazelcast.core;

import java.util.concurrent.TimeUnit;

public interface AsyncCountDownLatch extends ICountDownLatch {

    CompletionFuture<Boolean> asyncAwait(long timeout, TimeUnit unit) throws InterruptedException;

    CompletionFuture<Void> asyncCountDown();

    CompletionFuture<Integer> asyncGetCount();

    CompletionFuture<Boolean> asyncTrySetCount(int count);
}
