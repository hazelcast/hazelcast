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

import com.hazelcast.concurrent.semaphore.operations.AcquireOperation;
import com.hazelcast.concurrent.semaphore.operations.AvailableOperation;
import com.hazelcast.concurrent.semaphore.operations.DrainOperation;
import com.hazelcast.concurrent.semaphore.operations.InitOperation;
import com.hazelcast.concurrent.semaphore.operations.ReduceOperation;
import com.hazelcast.concurrent.semaphore.operations.ReleaseOperation;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.rethrowAllowInterrupted;
import static com.hazelcast.util.ValidationUtil.isNotNegative;

public class SemaphoreProxy extends AbstractDistributedObject<SemaphoreService> implements ISemaphore {

    private final String name;
    private final int partitionId;

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
        isNotNegative(permits, "permits");
        InitOperation operation = new InitOperation(name, permits);
        InternalCompletableFuture<Boolean> future = invoke(operation);
        return future.getSafely();
    }

    @Override
    public void acquire() throws InterruptedException {
        acquire(1);
    }

    @Override
    public void acquire(int permits) throws InterruptedException {
        isNotNegative(permits, "permits");
        try {
            AcquireOperation operation = new AcquireOperation(name, permits, -1);
            InternalCompletableFuture<Object> future = invoke(operation);
            future.get();
        } catch (Throwable t) {
            throw rethrowAllowInterrupted(t);
        }
    }

    @Override
    public int availablePermits() {
        AvailableOperation operation = new AvailableOperation(name);
        InternalCompletableFuture<Integer> future = invoke(operation);
        return future.getSafely();
    }

    @Override
    public int drainPermits() {
        DrainOperation operation = new DrainOperation(name);
        InternalCompletableFuture<Integer> future = invoke(operation);
        return future.getSafely();
    }

    @Override
    public void reducePermits(int reduction) {
        isNotNegative(reduction, "reduction");
        ReduceOperation operation = new ReduceOperation(name, reduction);
        InternalCompletableFuture<Object> future = invoke(operation);
        future.getSafely();
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        isNotNegative(permits, "permits");
        ReleaseOperation operation = new ReleaseOperation(name, permits);
        InternalCompletableFuture future = invoke(operation);
        future.getSafely();
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
        isNotNegative(permits, "permits");
        try {
            AcquireOperation operation = new AcquireOperation(name, permits, unit.toMillis(timeout));
            Future<Boolean> future = invoke(operation);
            return future.get();
        } catch (Throwable t) {
            throw rethrowAllowInterrupted(t);
        }
    }

    private <T> InternalCompletableFuture<T> invoke(Operation operation) {
        NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        //noinspection unchecked
        return (InternalCompletableFuture) operationService.invokeOnPartition(
                SemaphoreService.SERVICE_NAME, operation, partitionId);
    }

    @Override
    public String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ISemaphore{");
        sb.append("name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
