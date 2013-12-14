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

package com.hazelcast.concurrent.atomiclong.proxy;

import com.hazelcast.concurrent.atomiclong.*;
import com.hazelcast.core.AsyncAtomicLong;
import com.hazelcast.core.CompletableFuture;
import com.hazelcast.core.Function;
import com.hazelcast.spi.*;
import com.hazelcast.util.ExceptionUtil;

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * User: sancar
 * Date: 2/26/13
 * Time: 12:22 PM
 */
public class AtomicLongProxy extends AbstractDistributedObject<AtomicLongService> implements AsyncAtomicLong {

    private final String name;
    private final int partitionId;

    public AtomicLongProxy(String name, NodeEngine nodeEngine, AtomicLongService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    private <E> InternalCompletableFuture<E> asyncInvoke(Operation operation) {
        //todo:
        //we are setting the service here explicitly, but this should be done in the invokeOnPartition method
        //where the service should be passed as argument, instead of the service-name.
        operation.setService(getService());
        try {
            return (InternalCompletableFuture<E>)getNodeEngine().getOperationService().invokeOnPartition(AtomicLongService.SERVICE_NAME,operation,partitionId);
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
        return addAndGet(-1);
    }

    @Override
    public CompletableFuture<Long> asyncDecrementAndGet() {
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
        return addAndGet(1);
    }

    @Override
    public CompletableFuture<Long> asyncIncrementAndGet() {
        return asyncAddAndGet(1);
    }

    @Override
    public long getAndIncrement() {
        return getAndAdd(1);
    }

    @Override
    public CompletableFuture<Long> asyncGetAndIncrement() {
        return asyncGetAndAdd(1);
    }

    @Override
    public void alter(Function<Long, Long> function) {
        asyncAlter(function).getSafely();
    }

    @Override
    public InternalCompletableFuture<Void> asyncAlter(Function<Long, Long> function) {
        isNotNull(function, "function");

        Operation operation = new AlterOperation(name,function);
        return asyncInvoke(operation);
    }

    @Override
    public long alterAndGet(Function<Long, Long> function) {
        return asyncAlterAndGet(function).getSafely();
    }

    @Override
    public InternalCompletableFuture<Long> asyncAlterAndGet(Function<Long, Long> function) {
        isNotNull(function, "function");

        Operation operation = new AlterAndGetOperation(name, function);
        return asyncInvoke(operation);
    }

    @Override
    public long getAndAlter(Function<Long, Long> function) {
        return asyncGetAndAlter(function).getSafely();
    }

    @Override
    public InternalCompletableFuture<Long> asyncGetAndAlter(Function<Long, Long> function) {
        isNotNull(function, "function");

        Operation operation = new GetAndAlterOperation(name, function);
        return asyncInvoke(operation);
    }

    @Override
    public <R> R apply(Function<Long, R> function) {
        return asyncApply(function).getSafely();
    }

    @Override
    public <R> InternalCompletableFuture<R> asyncApply(Function<Long, R> function) {
        isNotNull(function, "function");

        Operation operation = new ApplyOperation<R>(name,function);
        return asyncInvoke(operation);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IAtomicLong{");
        sb.append("name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
