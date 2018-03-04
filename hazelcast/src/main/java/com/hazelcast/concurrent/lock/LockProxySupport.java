/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.concurrent.lock;

import com.hazelcast.concurrent.lock.operations.GetLockCountOperation;
import com.hazelcast.concurrent.lock.operations.GetRemainingLeaseTimeOperation;
import com.hazelcast.concurrent.lock.operations.IsLockedOperation;
import com.hazelcast.concurrent.lock.operations.LockOperation;
import com.hazelcast.concurrent.lock.operations.UnlockOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.concurrent.lock.LockServiceImpl.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.rethrowAllowInterrupted;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static java.lang.Thread.currentThread;

public final class LockProxySupport {

    private final ObjectNamespace namespace;
    private final long maxLeaseTimeInMillis;

    public LockProxySupport(ObjectNamespace namespace, long maxLeaseTimeInMillis) {
        this.namespace = namespace;
        this.maxLeaseTimeInMillis = maxLeaseTimeInMillis;
    }

    public boolean isLocked(NodeEngine nodeEngine, Data key) {
        IsLockedOperation operation = new IsLockedOperation(namespace, key);
        InternalCompletableFuture<Boolean> f = invoke(nodeEngine, operation, key);
        return f.join();
    }

    private InternalCompletableFuture invoke(NodeEngine nodeEngine, Operation operation, Data key) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        return nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
    }

    public boolean isLockedByCurrentThread(NodeEngine nodeEngine, Data key) {
        IsLockedOperation operation = new IsLockedOperation(namespace, key, getThreadId());
        InternalCompletableFuture<Boolean> f = invoke(nodeEngine, operation, key);
        return f.join();
    }

    public int getLockCount(NodeEngine nodeEngine, Data key) {
        Operation operation = new GetLockCountOperation(namespace, key);
        InternalCompletableFuture<Number> f = invoke(nodeEngine, operation, key);
        return f.join().intValue();
    }

    public long getRemainingLeaseTime(NodeEngine nodeEngine, Data key) {
        Operation operation = new GetRemainingLeaseTimeOperation(namespace, key);
        InternalCompletableFuture<Number> f = invoke(nodeEngine, operation, key);
        return f.join().longValue();
    }

    public void lock(NodeEngine nodeEngine, Data key) {
        lock(nodeEngine, key, -1);
    }

    public void lock(NodeEngine nodeEngine, Data key, long leaseTime) {
        leaseTime = getLeaseTime(leaseTime);

        LockOperation operation = new LockOperation(namespace, key, getThreadId(), leaseTime, -1);
        InternalCompletableFuture<Boolean> f = invoke(nodeEngine, operation, key);
        if (!f.join()) {
            throw new IllegalStateException();
        }
    }

    public void lockInterruptly(NodeEngine nodeEngine, Data key) throws InterruptedException {
        lockInterruptly(nodeEngine, key, -1);
    }

    public void lockInterruptly(NodeEngine nodeEngine, Data key, long leaseTime) throws InterruptedException {
        leaseTime = getLeaseTime(leaseTime);

        LockOperation operation = new LockOperation(namespace, key, getThreadId(), leaseTime, -1);
        InternalCompletableFuture<Boolean> f = invoke(nodeEngine, operation, key);
        try {
            f.get();
        } catch (Throwable t) {
            throw rethrowAllowInterrupted(t);
        }
    }

    private long getLeaseTime(long leaseTime) {
        if (leaseTime > maxLeaseTimeInMillis) {
            throw new IllegalArgumentException("Max allowed lease time: " + maxLeaseTimeInMillis + "ms. "
                    + "Given lease time: " + leaseTime + "ms.");
        }
        if (leaseTime < 0) {
            leaseTime = maxLeaseTimeInMillis;
        }
        return leaseTime;
    }

    public boolean tryLock(NodeEngine nodeEngine, Data key) {
        try {
            return tryLock(nodeEngine, key, 0, TimeUnit.MILLISECONDS, -1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return false;
        }
    }

    public boolean tryLock(NodeEngine nodeEngine, Data key, long timeout, TimeUnit timeunit) throws InterruptedException {
        return tryLock(nodeEngine, key, timeout, timeunit, -1, TimeUnit.MILLISECONDS);
    }

    public boolean tryLock(NodeEngine nodeEngine, Data key, long timeout, TimeUnit timeunit,
                           long leaseTime, TimeUnit leaseTimeunit) throws InterruptedException {
        long timeoutInMillis = getTimeInMillis(timeout, timeunit);
        long leaseTimeInMillis = getTimeInMillis(leaseTime, leaseTimeunit);
        LockOperation operation = new LockOperation(namespace, key, getThreadId(), leaseTimeInMillis, timeoutInMillis);
        InternalCompletableFuture<Boolean> f = invoke(nodeEngine, operation, key);

        try {
            return f.get();
        } catch (Throwable t) {
            throw rethrowAllowInterrupted(t);
        }
    }

    private long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    public void unlock(NodeEngine nodeEngine, Data key) {
        UnlockOperation operation = new UnlockOperation(namespace, key, getThreadId());
        InternalCompletableFuture<Number> f = invoke(nodeEngine, operation, key);
        f.join();
    }

    public void forceUnlock(NodeEngine nodeEngine, Data key) {
        UnlockOperation operation = new UnlockOperation(namespace, key, -1, true);
        InternalCompletableFuture<Number> f = invoke(nodeEngine, operation, key);
        f.join();
    }

    public ObjectNamespace getNamespace() {
        return namespace;
    }
}
