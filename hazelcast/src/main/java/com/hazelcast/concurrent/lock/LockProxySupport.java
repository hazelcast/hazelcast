/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.util.ThreadUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.concurrent.lock.LockService.SERVICE_NAME;

/**
 * @mdogan 2/12/13
 */
public final class LockProxySupport {

    private final ObjectNamespace namespace;

    public LockProxySupport(ObjectNamespace namespace) {
        this.namespace = namespace;
    }

    public boolean isLocked(NodeEngine nodeEngine, Data key) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        IsLockedOperation operation = new IsLockedOperation(namespace, key);
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                    .build();
            Future future = invocation.invoke();
            return (Boolean) future.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void lock(NodeEngine nodeEngine, Data key) {
        lock(nodeEngine, key, -1);
    }

    public void lock(NodeEngine nodeEngine, Data key, long ttl) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        LockOperation operation = new LockOperation(namespace, key, ThreadUtil.getThreadId(), ttl, -1);
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                    .build();
            Future future = invocation.invoke();
            Boolean result = (Boolean) future.get();
            if (!result) {
                throw new IllegalStateException();
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public boolean tryLock(NodeEngine nodeEngine, Data key) {
        try {
            return tryLock(nodeEngine, key, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryLock(NodeEngine nodeEngine, Data key, long timeout, TimeUnit timeunit) throws InterruptedException {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        LockOperation operation = new LockOperation(namespace, key, ThreadUtil.getThreadId(),
                getTimeInMillis(timeout, timeunit));
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                    .build();
            Future future = invocation.invoke();
            return (Boolean) future.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowInterrupted(t);
        }
    }

    private long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    public void unlock(NodeEngine nodeEngine, Data key) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        UnlockOperation operation = new UnlockOperation(namespace, key, ThreadUtil.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                    .build();
            Future future = invocation.invoke();
            future.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void forceUnlock(NodeEngine nodeEngine, Data key) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        UnlockOperation operation = new UnlockOperation(namespace, key, -1, true);
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                    .build();
            Future future = invocation.invoke();
            future.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }
}
