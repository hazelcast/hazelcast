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

package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.concurrent.countdownlatch.operations.AwaitOperation;
import com.hazelcast.concurrent.countdownlatch.operations.CountDownOperation;
import com.hazelcast.concurrent.countdownlatch.operations.GetCountOperation;
import com.hazelcast.concurrent.countdownlatch.operations.SetCountOperation;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.rethrowAllowInterrupted;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class CountDownLatchProxy extends AbstractDistributedObject<CountDownLatchService> implements ICountDownLatch {

    private final String name;
    private final int partitionId;

    public CountDownLatchProxy(String name, NodeEngine nodeEngine) {
        super(nodeEngine, null);
        this.name = name;
        Data nameAsPartitionAwareData = getNameAsPartitionAwareData();
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(nameAsPartitionAwareData);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        checkNotNull(unit, "unit can't be null");

        Operation op = new AwaitOperation(name, unit.toMillis(timeout))
                .setPartitionId(partitionId);
        Future<Boolean> f = invokeOnPartition(op);
        try {
            return f.get();
        } catch (ExecutionException e) {
            throw rethrowAllowInterrupted(e);
        }
    }

    @Override
    public void countDown() {
        Operation op = new CountDownOperation(name)
                .setPartitionId(partitionId);
        InternalCompletableFuture f = invokeOnPartition(op);
        f.join();
    }

    @Override
    public int getCount() {
        Operation op = new GetCountOperation(name)
                .setPartitionId(partitionId);
        InternalCompletableFuture<Integer> f = invokeOnPartition(op);
        return f.join();
    }

    @Override
    public boolean trySetCount(int count) {
        checkNotNegative(count, "count can't be negative");

        Operation op = new SetCountOperation(name, count)
                .setPartitionId(partitionId);
        InternalCompletableFuture<Boolean> f = invokeOnPartition(op);
        return f.join();
    }

    @Override
    public String getServiceName() {
        return CountDownLatchService.SERVICE_NAME;
    }

    @Override
    public String toString() {
        return "ICountDownLatch{name='" + name + '\'' + '}';
    }
}
