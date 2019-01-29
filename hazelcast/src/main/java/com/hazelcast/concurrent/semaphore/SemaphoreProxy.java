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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.concurrent.semaphore.operations.AcquireOperation;
import com.hazelcast.concurrent.semaphore.operations.AvailableOperation;
import com.hazelcast.concurrent.semaphore.operations.DrainOperation;
import com.hazelcast.concurrent.semaphore.operations.InitOperation;
import com.hazelcast.concurrent.semaphore.operations.IncreaseOperation;
import com.hazelcast.concurrent.semaphore.operations.ReduceOperation;
import com.hazelcast.concurrent.semaphore.operations.ReleaseOperation;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.rethrowAllowInterrupted;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static java.lang.Thread.currentThread;

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
        checkNotNegative(permits, "permits can't be negative");

        Operation operation = new InitOperation(name, permits)
                .setPartitionId(partitionId);
        InternalCompletableFuture<Boolean> future = invokeOnPartition(operation);
        return future.join();
    }

    @Override
    public void acquire() throws InterruptedException {
        acquire(1);
    }

    @Override
    public void acquire(int permits) throws InterruptedException {
        checkNotNegative(permits, "permits can't be negative");

        try {
            Operation operation = new AcquireOperation(name, permits, -1)
                    .setPartitionId(partitionId);
            InternalCompletableFuture<Object> future = invokeOnPartition(operation);
            future.get();
        } catch (Throwable t) {
            throw rethrowAllowInterrupted(t);
        }
    }

    @Override
    public int availablePermits() {
        Operation operation = new AvailableOperation(name)
                .setPartitionId(partitionId);
        InternalCompletableFuture<Integer> future = invokeOnPartition(operation);
        return future.join();
    }

    @Override
    public int drainPermits() {
        Operation operation = new DrainOperation(name)
                .setPartitionId(partitionId);
        InternalCompletableFuture<Integer> future = invokeOnPartition(operation);
        return future.join();
    }

    @Override
    public void reducePermits(int reduction) {
        checkNotNegative(reduction, "reduction can't be negative");

        Operation operation = new ReduceOperation(name, reduction)
                .setPartitionId(partitionId);
        InternalCompletableFuture<Object> future = invokeOnPartition(operation);
        future.join();
    }

    @Override
    public void increasePermits(int increase) {
        if (getNodeEngine().getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
            throw new UnsupportedOperationException("Increasing permits is available when cluster version is 3.10 or higher");
        }

        checkNotNegative(increase, "increase can't be negative");

        Operation operation = new IncreaseOperation(name, increase)
                .setPartitionId(partitionId);
        InternalCompletableFuture<Object> future = invokeOnPartition(operation);
        future.join();
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        checkNotNegative(permits, "permits can't be negative");

        Operation operation = new ReleaseOperation(name, permits)
                .setPartitionId(partitionId);
        InternalCompletableFuture future = invokeOnPartition(operation);
        future.join();
    }

    @Override
    public boolean tryAcquire() {
        try {
            return tryAcquire(1, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return false;
        }
    }

    @Override
    public boolean tryAcquire(int permits) {
        try {
            return tryAcquire(permits, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return false;
        }
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return tryAcquire(1, timeout, unit);
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
        checkNotNegative(permits, "permits can't be negative");

        try {
            Operation operation = new AcquireOperation(name, permits, unit.toMillis(timeout))
                    .setPartitionId(partitionId);
            Future<Boolean> future = invokeOnPartition(operation);
            return future.get();
        } catch (Throwable t) {
            throw rethrowAllowInterrupted(t);
        }
    }

    @Override
    public String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    @Override
    public String toString() {
        return "ISemaphore{name='" + name + '\'' + '}';
    }
}
