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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.core.ISemaphore;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @ali 1/22/13
 */
public class SemaphoreProxy extends AbstractDistributedObject<SemaphoreService> implements ISemaphore {

    final String name;

    final int partitionId;

    public SemaphoreProxy(String name, SemaphoreService service, NodeEngine nodeEngine) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(nodeEngine.toData(name));
    }

    public String getName() {
        return name;
    }

    public boolean init(int permits) {
        checkNegative(permits);
        try {
            return (Boolean) invoke(new InitOperation(name, permits));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void acquire() throws InterruptedException {
        acquire(1);
    }

    public void acquire(int permits) throws InterruptedException {
        checkNegative(permits);
        try {
            invoke(new AcquireOperation(name, permits, -1));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowInterrupted(t);
        }
    }

    public int availablePermits() {
        try {
            return (Integer) invoke(new AvailableOperation(name));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public int drainPermits() {
        try {
            return (Integer) invoke(new DrainOperation(name));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void reducePermits(int reduction) {
        checkNegative(reduction);
        try {
            invoke(new ReduceOperation(name, reduction));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void release() {
        release(1);
    }

    public void release(int permits) {
        checkNegative(permits);
        try {
            invoke(new ReleaseOperation(name, permits));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public boolean tryAcquire() {
        try {
            return tryAcquire(1, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryAcquire(int permits) {
        try {
            return tryAcquire(permits, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return tryAcquire(1, timeout, unit);
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
        checkNegative(permits);
        try {
            return (Boolean) invoke(new AcquireOperation(name, permits, unit.toMillis(timeout)));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowInterrupted(t);
        }
    }

    public Object getId() {
        return name;
    }

    private <T> T invoke(SemaphoreOperation operation) throws ExecutionException, InterruptedException {
        final NodeEngine nodeEngine = getNodeEngine();
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(SemaphoreService.SERVICE_NAME, operation, partitionId).build();
        Future f = inv.invoke();
        return (T) nodeEngine.toObject(f.get());
    }

    private void checkNegative(int permits) {
        if (permits < 0) {
            throw new IllegalArgumentException("Permits cannot be negative!");
        }
    }

    public String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }
}
