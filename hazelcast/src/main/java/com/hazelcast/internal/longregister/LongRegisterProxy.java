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

package com.hazelcast.internal.longregister;

import com.hazelcast.core.IFunction;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.internal.longregister.operations.AddAndGetOperation;
import com.hazelcast.internal.longregister.operations.GetAndAddOperation;
import com.hazelcast.internal.longregister.operations.GetAndSetOperation;
import com.hazelcast.internal.longregister.operations.GetOperation;
import com.hazelcast.internal.longregister.operations.SetOperation;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

/**
 * Partially implements {@link IAtomicLong}.
 */
@SuppressWarnings("checkstyle:methodcount")
public class LongRegisterProxy extends AbstractDistributedObject<LongRegisterService> implements IAtomicLong {

    private final String name;
    private final int partitionId;

    public LongRegisterProxy(String name, NodeEngine nodeEngine, LongRegisterService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
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
        return LongRegisterService.SERVICE_NAME;
    }

    @Override
    public long addAndGet(long delta) {
        return addAndGetAsync(delta).joinInternal();
    }

    @Override
    public InvocationFuture<Long> addAndGetAsync(long delta) {
        Operation operation = new AddAndGetOperation(name, delta).setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long newValue) {
        setAsync(newValue).joinInternal();
    }

    @Override
    public InvocationFuture<Void> setAsync(long newValue) {
        Operation operation = new SetOperation(name, newValue)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public long getAndSet(long newValue) {
        return getAndSetAsync(newValue).joinInternal();
    }

    @Override
    public InternalCompletableFuture<Long> getAndSetAsync(long newValue) {
        Operation operation = new GetAndSetOperation(name, newValue)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public long getAndAdd(long delta) {
        return getAndAddAsync(delta).joinInternal();
    }

    @Override
    public InternalCompletableFuture<Long> getAndAddAsync(long delta) {
        Operation operation = new GetAndAddOperation(name, delta)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public long decrementAndGet() {
        return decrementAndGetAsync().joinInternal();
    }

    @Override
    public long getAndDecrement() {
        return getAndDecrementAsync().joinInternal();
    }

    @Override
    public InternalCompletableFuture<Long> decrementAndGetAsync() {
        return addAndGetAsync(-1);
    }

    @Override
    public InternalCompletableFuture<Long> getAndDecrementAsync() {
        return getAndAddAsync(-1);
    }

    @Override
    public long get() {
        return getAsync().joinInternal();
    }

    @Override
    public InternalCompletableFuture<Long> getAsync() {
        Operation operation = new GetOperation(name)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public long incrementAndGet() {
        return incrementAndGetAsync().joinInternal();
    }

    @Override
    public InternalCompletableFuture<Long> incrementAndGetAsync() {
        return addAndGetAsync(1);
    }

    @Override
    public long getAndIncrement() {
        return getAndIncrementAsync().joinInternal();
    }

    @Override
    public InternalCompletableFuture<Long> getAndIncrementAsync() {
        return getAndAddAsync(1);
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalCompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalCompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<Long, R> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "LongRegister{" + "name='" + name + '\'' + '}';
    }
}
