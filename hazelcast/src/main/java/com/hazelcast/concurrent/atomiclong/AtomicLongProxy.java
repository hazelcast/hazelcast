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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.concurrent.atomiclong.operations.AddAndGetOperation;
import com.hazelcast.concurrent.atomiclong.operations.AlterAndGetOperation;
import com.hazelcast.concurrent.atomiclong.operations.AlterOperation;
import com.hazelcast.concurrent.atomiclong.operations.ApplyOperation;
import com.hazelcast.concurrent.atomiclong.operations.CompareAndSetOperation;
import com.hazelcast.concurrent.atomiclong.operations.GetAndAddOperation;
import com.hazelcast.concurrent.atomiclong.operations.GetAndAlterOperation;
import com.hazelcast.concurrent.atomiclong.operations.GetAndSetOperation;
import com.hazelcast.concurrent.atomiclong.operations.GetOperation;
import com.hazelcast.concurrent.atomiclong.operations.SetOperation;
import com.hazelcast.core.AsyncAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;

import static com.hazelcast.util.ValidationUtil.isNotNull;

public class AtomicLongProxy extends AbstractDistributedObject<AtomicLongService> implements AsyncAtomicLong {

    private final String name;
    private final int partitionId;

    public AtomicLongProxy(String name, NodeEngine nodeEngine, AtomicLongService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    private <E> InternalCompletableFuture<E> asyncInvoke(Operation operation) {
        try {
            OperationService operationService = getNodeEngine().getOperationService();
            //noinspection unchecked
            return (InternalCompletableFuture<E>) operationService.invokeOnPartition(
                    AtomicLongService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String getServiceName() {
        return AtomicLongService.SERVICE_NAME;
    }

    @Override
    public long addAndGet(long delta) {
        return asyncAddAndGet(delta).getSafely();
    }

    @Override
    public InternalCompletableFuture<Long> asyncAddAndGet(long delta) {
        Operation operation = new AddAndGetOperation(name, delta);
        return asyncInvoke(operation);
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        return asyncCompareAndSet(expect, update).getSafely();
    }

    @Override
    public InternalCompletableFuture<Boolean> asyncCompareAndSet(long expect, long update) {
        Operation operation = new CompareAndSetOperation(name, expect, update);
        return asyncInvoke(operation);
    }

    @Override
    public void set(long newValue) {
        asyncSet(newValue).getSafely();
    }

    @Override
    public InternalCompletableFuture<Void> asyncSet(long newValue) {
        Operation operation = new SetOperation(name, newValue);
        return asyncInvoke(operation);
    }

    @Override
    public long getAndSet(long newValue) {
        return asyncGetAndSet(newValue).getSafely();
    }

    @Override
    public InternalCompletableFuture<Long> asyncGetAndSet(long newValue) {
        Operation operation = new GetAndSetOperation(name, newValue);
        return asyncInvoke(operation);
    }

    @Override
    public long getAndAdd(long delta) {
        return asyncGetAndAdd(delta).getSafely();
    }

    @Override
    public InternalCompletableFuture<Long> asyncGetAndAdd(long delta) {
        Operation operation = new GetAndAddOperation(name, delta);
        return asyncInvoke(operation);
    }

    @Override
    public long decrementAndGet() {
        return asyncDecrementAndGet().getSafely();
    }

    @Override
    public InternalCompletableFuture<Long> asyncDecrementAndGet() {
        return asyncAddAndGet(-1);
    }

    @Override
    public long get() {
        return asyncGet().getSafely();
    }

    @Override
    public InternalCompletableFuture<Long> asyncGet() {
        GetOperation operation = new GetOperation(name);
        return asyncInvoke(operation);
    }

    @Override
    public long incrementAndGet() {
        return asyncIncrementAndGet().getSafely();
    }

    @Override
    public InternalCompletableFuture<Long> asyncIncrementAndGet() {
        return asyncAddAndGet(1);
    }

    @Override
    public long getAndIncrement() {
        return asyncGetAndIncrement().getSafely();
    }

    @Override
    public InternalCompletableFuture<Long> asyncGetAndIncrement() {
        return asyncGetAndAdd(1);
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        asyncAlter(function).getSafely();
    }

    @Override
    public InternalCompletableFuture<Void> asyncAlter(IFunction<Long, Long> function) {
        isNotNull(function, "function");

        Operation operation = new AlterOperation(name, function);
        return asyncInvoke(operation);
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        return asyncAlterAndGet(function).getSafely();
    }

    @Override
    public InternalCompletableFuture<Long> asyncAlterAndGet(IFunction<Long, Long> function) {
        isNotNull(function, "function");

        Operation operation = new AlterAndGetOperation(name, function);
        return asyncInvoke(operation);
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        return asyncGetAndAlter(function).getSafely();
    }

    @Override
    public InternalCompletableFuture<Long> asyncGetAndAlter(IFunction<Long, Long> function) {
        isNotNull(function, "function");

        Operation operation = new GetAndAlterOperation(name, function);
        return asyncInvoke(operation);
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        return asyncApply(function).getSafely();
    }

    @Override
    public <R> InternalCompletableFuture<R> asyncApply(IFunction<Long, R> function) {
        isNotNull(function, "function");

        Operation operation = new ApplyOperation<R>(name, function);
        return asyncInvoke(operation);
    }

    @Override
    public String toString() {
        return "IAtomicLong{" + "name='" + name + '\'' + '}';
    }
}
