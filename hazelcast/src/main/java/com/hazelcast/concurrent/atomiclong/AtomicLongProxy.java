/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import static com.hazelcast.util.Preconditions.isNotNull;

public class AtomicLongProxy extends AbstractDistributedObject<AtomicLongService> implements IAtomicLong {
    private final String name;
    private final int partitionId;

    public AtomicLongProxy(String name, NodeEngine nodeEngine, AtomicLongService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    // ============= addAndGet =========================================

    @Override
    public long addAndGet(long delta) {
        return addAndGetAsync0(delta, SYNC).join();
    }

    @Override
    public InternalCompletableFuture<Long> addAndGetAsync(long delta) {
        return addAndGetAsync0(delta, ASYNC);
    }

    private InternalCompletableFuture<Long> addAndGetAsync0(long delta, boolean sync) {
        Operation operation = new AddAndGetOperation(name, delta)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation, sync);
    }

    // ============= compareAndSet =========================================

    @Override
    public boolean compareAndSet(long expect, long update) {
        return compareAndSetAsync0(expect, update, SYNC).join();
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        return compareAndSetAsync0(expect, update, ASYNC);
    }

    private InternalCompletableFuture<Boolean> compareAndSetAsync0(long expect, long update, boolean sync) {
        Operation operation = new CompareAndSetOperation(name, expect, update)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation, sync);
    }

    // ============= set =========================================

    @Override
    public void set(long newValue) {
        setAsync0(SYNC, newValue).join();
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(long newValue) {
        return setAsync0(ASYNC, newValue);
    }

    private InternalCompletableFuture<Void> setAsync0(boolean sync, long newValue) {
        Operation operation = new SetOperation(name, newValue)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation, sync);
    }

    // ============= getAndSet =========================================

    @Override
    public long getAndSet(long newValue) {
        return getAndSetAsync0(newValue, SYNC).join();
    }

    @Override
    public InternalCompletableFuture<Long> getAndSetAsync(long newValue) {
        return getAndSetAsync0(newValue, ASYNC);
    }

    private InternalCompletableFuture<Long> getAndSetAsync0(long newValue, boolean sync) {
        Operation operation = new GetAndSetOperation(name, newValue)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation, sync);
    }

    // ============= getAndAdd =========================================

    @Override
    public long getAndAdd(long delta) {
        return getAndAddAsync0(delta, SYNC).join();
    }

    @Override
    public InternalCompletableFuture<Long> getAndAddAsync(long delta) {
        return getAndAddAsync0(delta, ASYNC);
    }

    private InternalCompletableFuture<Long> getAndAddAsync0(long delta, boolean sync) {
        Operation operation = new GetAndAddOperation(name, delta)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation, sync);
    }

    // ============= decrementAndGet =========================================

    @Override
    public long decrementAndGet() {
        return addAndGet(-1);
    }

    @Override
    public InternalCompletableFuture<Long> decrementAndGetAsync() {
        return addAndGetAsync(-1);
    }

    // ============= get =========================================

    @Override
    public long get() {
        return getAsync0(SYNC).join();
    }

    @Override
    public InternalCompletableFuture<Long> getAsync() {
        return getAsync0(ASYNC);
    }

    private InternalCompletableFuture<Long> getAsync0(boolean sync) {
        Operation operation = new GetOperation(name)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation, sync);
    }

    // ============= incrementAndGet =========================================

    @Override
    public long incrementAndGet() {
        return addAndGet(1);
    }

    @Override
    public InternalCompletableFuture<Long> incrementAndGetAsync() {
        return addAndGetAsync(1);
    }

    // ============= getAndIncrement =========================================

    @Override
    public long getAndIncrement() {
        return getAndAdd(1);
    }

    @Override
    public InternalCompletableFuture<Long> getAndIncrementAsync() {
        return getAndAddAsync(1);
    }

    // ============= alter =========================================

    @Override
    public void alter(IFunction<Long, Long> function) {
        alterAsync0(function, SYNC).join();
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        return alterAsync0(function, ASYNC);
    }

    private InternalCompletableFuture<Void> alterAsync0(IFunction<Long, Long> function, boolean sync) {
        isNotNull(function, "function");

        Operation operation = new AlterOperation(name, function)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation, sync);
    }

    // ============= alterAndGet =========================================

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        return alterAndGetAsync0(function, SYNC).join();
    }

    @Override
    public InternalCompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        return alterAndGetAsync0(function, ASYNC);
    }

    private InternalCompletableFuture<Long> alterAndGetAsync0(IFunction<Long, Long> function, boolean sync) {
        isNotNull(function, "function");

        Operation operation = new AlterAndGetOperation(name, function)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation, sync);
    }

    // ============= getAndAlter =========================================

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        return getAndAlterAsync0(function, SYNC).join();
    }

    @Override
    public InternalCompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        return getAndAlterAsync0(function, ASYNC);
    }

    private InternalCompletableFuture<Long> getAndAlterAsync0(IFunction<Long, Long> function, boolean sync) {
        isNotNull(function, "function");

        Operation operation = new GetAndAlterOperation(name, function)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation, sync);
    }

    // ============= apply =========================================

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        return applyAsync0(function, SYNC).join();
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<Long, R> function) {
        return applyAsync0(function, ASYNC);
    }

    private <R> InternalCompletableFuture<R> applyAsync0(IFunction<Long, R> function, boolean sync) {
        isNotNull(function, "function");

        Operation operation = new ApplyOperation<R>(name, function)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation, sync);
    }

    // ============= misc =========================================

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
    public String toString() {
        return "IAtomicLong{" + "name='" + name + '\'' + '}';
    }
}
