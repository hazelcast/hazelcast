package com.hazelcast.core;

import java.util.concurrent.TimeUnit;

public interface AsyncLock extends ILock {

    CompletionFuture<Void> asyncLock();

    CompletionFuture<Boolean> asyncTryLock();

    CompletionFuture<Boolean> asyncTryLock(long time, TimeUnit unit);

    CompletionFuture<Void> asyncUnlock();

    CompletionFuture<Void> asyncLock(long leaseTime, TimeUnit timeUnit);

    CompletionFuture<Void> asyncForceUnlock();

    CompletionFuture<Boolean> asyncIsLocked();

    CompletionFuture<Boolean> asyncIsLockedByCurrentThread();

    CompletionFuture<Integer> asyncGetLockCount();

    CompletionFuture<Long> asyncGetRemainingLeaseTime();

    CompletionFuture<Void> asyncLockInterruptibly();

}
