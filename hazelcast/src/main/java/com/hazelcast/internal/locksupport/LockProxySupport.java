/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.locksupport;

import com.hazelcast.internal.locksupport.operations.IsLockedOperation;
import com.hazelcast.internal.locksupport.operations.LockOperation;
import com.hazelcast.internal.locksupport.operations.UnlockOperation;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.locksupport.LockSupportServiceImpl.SERVICE_NAME;
import static com.hazelcast.internal.util.ExceptionUtil.rethrowAllowInterrupted;
import static com.hazelcast.internal.util.ThreadUtil.getThreadId;
import static com.hazelcast.internal.util.TimeUtil.timeInMsOrTimeIfNullUnit;
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
        return f.joinInternal();
    }

    private InternalCompletableFuture invoke(NodeEngine nodeEngine, Operation operation, Data key) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        return nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
    }

    public void lock(NodeEngine nodeEngine, Data key) {
        lock(nodeEngine, key, -1);
    }

    public void lock(NodeEngine nodeEngine, Data key, long leaseTime) {
        leaseTime = getLeaseTime(leaseTime);

        LockOperation operation = new LockOperation(namespace, key, getThreadId(), leaseTime, -1);
        InternalCompletableFuture<Boolean> f = invoke(nodeEngine, operation, key);
        if (!f.joinInternal()) {
            throw new IllegalStateException();
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

    public boolean tryLock(NodeEngine nodeEngine, Data key,
                           long timeout, @Nullable TimeUnit timeunit) throws InterruptedException {
        return tryLock(nodeEngine, key, timeout, timeunit, -1, TimeUnit.MILLISECONDS);
    }

    public boolean tryLock(NodeEngine nodeEngine, Data key,
                           long timeout, @Nullable TimeUnit timeunit,
                           long leaseTime, @Nullable TimeUnit leaseTimeunit) throws InterruptedException {
        long timeoutInMillis = timeInMsOrTimeIfNullUnit(timeout, timeunit);
        long leaseTimeInMillis = timeInMsOrTimeIfNullUnit(leaseTime, leaseTimeunit);
        LockOperation operation = new LockOperation(namespace, key, getThreadId(), leaseTimeInMillis, timeoutInMillis);
        InternalCompletableFuture<Boolean> f = invoke(nodeEngine, operation, key);

        try {
            return f.get();
        } catch (Throwable t) {
            throw rethrowAllowInterrupted(t);
        }
    }

    public void unlock(NodeEngine nodeEngine, Data key) {
        UnlockOperation operation = new UnlockOperation(namespace, key, getThreadId());
        InternalCompletableFuture<Number> f = invoke(nodeEngine, operation, key);
        f.joinInternal();
    }

    public void forceUnlock(NodeEngine nodeEngine, Data key) {
        UnlockOperation operation = new UnlockOperation(namespace, key, -1, true);
        InternalCompletableFuture<Number> f = invoke(nodeEngine, operation, key);
        f.joinInternal();
    }

    public ObjectNamespace getNamespace() {
        return namespace;
    }
}
