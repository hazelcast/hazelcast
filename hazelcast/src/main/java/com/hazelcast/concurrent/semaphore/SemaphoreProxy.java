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
import com.hazelcast.spi.*;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author ali 1/22/13
 */
public class SemaphoreProxy extends AbstractDistributedObject<SemaphoreService> implements ISemaphore {

    final String name;

    final int partitionId;

    public SemaphoreProxy(String name, SemaphoreService service, NodeEngine nodeEngine) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean init(int permits) {
        checkNegative(permits);
        try {
            return (Boolean) invoke(new InitOperation(name, permits));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public void acquire() throws InterruptedException {
        acquire(1);
    }

    @Override
    public void acquire(int permits) throws InterruptedException {
        checkNegative(permits);
        try {
            invoke(new AcquireOperation(name, permits, -1));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowInterrupted(t);
        }
    }

    @Override
    public int availablePermits() {
        try {
            return (Integer) invoke(new AvailableOperation(name));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public int drainPermits() {
        try {
            return (Integer) invoke(new DrainOperation(name));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public void reducePermits(int reduction) {
        checkNegative(reduction);
        try {
            invoke(new ReduceOperation(name, reduction));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        checkNegative(permits);
        try {
            invoke(new ReleaseOperation(name, permits));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public boolean tryAcquire() {
        try {
            return tryAcquire(1, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Override
    public boolean tryAcquire(int permits) {
        try {
            return tryAcquire(permits, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return tryAcquire(1, timeout, unit);
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
        checkNegative(permits);
        try {
            return (Boolean) invoke(new AcquireOperation(name, permits, unit.toMillis(timeout)));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowInterrupted(t);
        }
    }

    private <T> T invoke(SemaphoreOperation operation) throws ExecutionException, InterruptedException {
        final NodeEngine nodeEngine = getNodeEngine();
        Future f = nodeEngine.getOperationService().invokeOnPartition(SemaphoreService.SERVICE_NAME, operation, partitionId);
        return (T) f.get();
    }

    private void checkNegative(int permits) {
        if (permits < 0) {
            throw new IllegalArgumentException("Permits cannot be negative!");
        }
    }

    @Override
    public String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ISemaphore{");
        sb.append("name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
