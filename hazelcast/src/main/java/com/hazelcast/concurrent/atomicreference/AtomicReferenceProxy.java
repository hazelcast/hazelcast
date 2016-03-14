/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.concurrent.atomicreference.operations.AlterAndGetOperation;
import com.hazelcast.concurrent.atomicreference.operations.AlterOperation;
import com.hazelcast.concurrent.atomicreference.operations.ApplyOperation;
import com.hazelcast.concurrent.atomicreference.operations.CompareAndSetOperation;
import com.hazelcast.concurrent.atomicreference.operations.ContainsOperation;
import com.hazelcast.concurrent.atomicreference.operations.GetAndAlterOperation;
import com.hazelcast.concurrent.atomicreference.operations.GetAndSetOperation;
import com.hazelcast.concurrent.atomicreference.operations.GetOperation;
import com.hazelcast.concurrent.atomicreference.operations.IsNullOperation;
import com.hazelcast.concurrent.atomicreference.operations.SetAndGetOperation;
import com.hazelcast.concurrent.atomicreference.operations.SetOperation;
import com.hazelcast.core.AsyncAtomicReference;
import com.hazelcast.core.IFunction;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import static com.hazelcast.util.Preconditions.isNotNull;

public class AtomicReferenceProxy<E> extends AbstractDistributedObject<AtomicReferenceService>
        implements AsyncAtomicReference<E> {

    private final String name;
    private final int partitionId;

    public AtomicReferenceProxy(String name, NodeEngine nodeEngine, AtomicReferenceService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    @Override
    public void alter(IFunction<E, E> function) {
        asyncAlter(function).join();
    }

    @Override
    public InternalCompletableFuture<Void> asyncAlter(IFunction<E, E> function) {
        isNotNull(function, "function");

        Operation operation = new AlterOperation(name, toData(function))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public E alterAndGet(IFunction<E, E> function) {
        return asyncAlterAndGet(function).join();
    }

    @Override
    public InternalCompletableFuture<E> asyncAlterAndGet(IFunction<E, E> function) {
        isNotNull(function, "function");

        Operation operation = new AlterAndGetOperation(name, toData(function))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public E getAndAlter(IFunction<E, E> function) {
        return asyncGetAndAlter(function).join();
    }

    @Override
    public InternalCompletableFuture<E> asyncGetAndAlter(IFunction<E, E> function) {
        isNotNull(function, "function");

        Operation operation = new GetAndAlterOperation(name, toData(function))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public <R> R apply(IFunction<E, R> function) {
        return asyncApply(function).join();
    }

    @Override
    public <R> InternalCompletableFuture<R> asyncApply(IFunction<E, R> function) {
        isNotNull(function, "function");

        Operation operation = new ApplyOperation(name, toData(function))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public void clear() {
        asyncClear().join();
    }

    @Override
    public InternalCompletableFuture<Void> asyncClear() {
        return asyncSet(null);
    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        return asyncCompareAndSet(expect, update).join();
    }

    @Override
    public InternalCompletableFuture<Boolean> asyncCompareAndSet(E expect, E update) {
        Operation operation = new CompareAndSetOperation(name, toData(expect), toData(update))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public E get() {
        return asyncGet().join();
    }

    @Override
    public InternalCompletableFuture<E> asyncGet() {
        Operation operation = new GetOperation(name)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public boolean contains(E expected) {
        return asyncContains(expected).join();
    }

    @Override
    public InternalCompletableFuture<Boolean> asyncContains(E value) {
        Operation operation = new ContainsOperation(name, toData(value))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public void set(E newValue) {
        asyncSet(newValue).join();
    }

    @Override
    public InternalCompletableFuture<Void> asyncSet(E newValue) {
        Operation operation = new SetOperation(name, toData(newValue))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public E getAndSet(E newValue) {
        return asyncGetAndSet(newValue).join();
    }

    @Override
    public InternalCompletableFuture<E> asyncGetAndSet(E newValue) {
        Operation operation = new GetAndSetOperation(name, toData(newValue))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public E setAndGet(E update) {
        return asyncSetAndGet(update).join();
    }

    @Override
    public InternalCompletableFuture<E> asyncSetAndGet(E update) {
        Operation operation = new SetAndGetOperation(name, toData(update))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public boolean isNull() {
        return asyncIsNull().join();
    }

    @Override
    public InternalCompletableFuture<Boolean> asyncIsNull() {
        Operation operation = new IsNullOperation(name)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
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
        return AtomicReferenceService.SERVICE_NAME;
    }

    @Override
    public String toString() {
        return "IAtomicReference{" + "name='" + name + '\'' + '}';
    }
}
