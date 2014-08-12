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
import com.hazelcast.spi.OperationService;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.rethrowAllowInterrupted;

public class CountDownLatchProxy extends AbstractDistributedObject<CountDownLatchService> implements ICountDownLatch {

    private final String name;
    private final int partitionId;

    public CountDownLatchProxy(String name, NodeEngine nodeEngine) {
        super(nodeEngine, null);
        this.name = name;
        Data nameAsPartitionAwareData = getNameAsPartitionAwareData();
        partitionId = nodeEngine.getPartitionService().getPartitionId(nameAsPartitionAwareData);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        AwaitOperation op = new AwaitOperation(name, getTimeInMillis(timeout, unit));
        Future<Boolean> f = invoke(op);
        try {
            return f.get();
        } catch (ExecutionException e) {
            throw rethrowAllowInterrupted(e);
        }
    }

    private static long getTimeInMillis(long time, TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    @Override
    public void countDown() {
        CountDownOperation op = new CountDownOperation(name);
        InternalCompletableFuture f = invoke(op);
        f.getSafely();
    }

    @Override
    public int getCount() {
        GetCountOperation op = new GetCountOperation(name);
        InternalCompletableFuture<Integer> f = invoke(op);
        return f.getSafely();
    }

    @Override
    public boolean trySetCount(int count) {
        if(count < 0) {
            throw new IllegalArgumentException("count can't be negative");
        }
        SetCountOperation op = new SetCountOperation(name, count);
        InternalCompletableFuture<Boolean> f = invoke(op);
        return f.getSafely();
    }

    private InternalCompletableFuture invoke(Operation op) {
        NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.invokeOnPartition(CountDownLatchService.SERVICE_NAME, op, partitionId);
    }

    @Override
    public String getServiceName() {
        return CountDownLatchService.SERVICE_NAME;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ICountDownLatch{");
        sb.append("name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
