/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.unsafe.atomicreference;

import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.GetAndSetOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.AlterAndGetOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.AlterOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.ApplyOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.CompareAndSetOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.ContainsOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.GetAndAlterOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.GetOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.IsNullOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.SetOperation;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;

import static com.hazelcast.util.Preconditions.isNotNull;

public class AtomicReferenceProxy<E> extends AbstractDistributedObject<AtomicReferenceService>
        implements IAtomicReference<E> {

    private final String name;
    private final int partitionId;

    public AtomicReferenceProxy(String name, NodeEngine nodeEngine, AtomicReferenceService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    @Override
    public void alter(IFunction<E, E> function) {
        alterAsync(function).join();
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<E, E> function) {
        isNotNull(function, "function");

        Operation operation = new AlterOperation(name, toData(function))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public E alterAndGet(IFunction<E, E> function) {
        return alterAndGetAsync(function).join();
    }

    @Override
    public InternalCompletableFuture<E> alterAndGetAsync(IFunction<E, E> function) {
        isNotNull(function, "function");

        Operation operation = new AlterAndGetOperation(name, toData(function))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public E getAndAlter(IFunction<E, E> function) {
        return getAndAlterAsync(function).join();
    }

    @Override
    public InternalCompletableFuture<E> getAndAlterAsync(IFunction<E, E> function) {
        isNotNull(function, "function");

        Operation operation = new GetAndAlterOperation(name, toData(function))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public <R> R apply(IFunction<E, R> function) {
        return applyAsync(function).join();
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<E, R> function) {
        isNotNull(function, "function");

        Operation operation = new ApplyOperation(name, toData(function))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public void clear() {
        clearAsync().join();
    }

    @Override
    public InternalCompletableFuture<Void> clearAsync() {
        return setAsync(null);
    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        return compareAndSetAsync(expect, update).join();
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(E expect, E update) {
        Operation operation = new CompareAndSetOperation(name, toData(expect), toData(update))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public E get() {
        return getAsync().join();
    }

    @Override
    public InternalCompletableFuture<E> getAsync() {
        Operation operation = new GetOperation(name)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public boolean contains(E expected) {
        return containsAsync(expected).join();
    }

    @Override
    public InternalCompletableFuture<Boolean> containsAsync(E expected) {
        Operation operation = new ContainsOperation(name, toData(expected))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public void set(E newValue) {
        setAsync(newValue).join();
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(E newValue) {
        Operation operation = new SetOperation(name, toData(newValue))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public E getAndSet(E newValue) {
        return getAndSetAsync(newValue).join();
    }

    @Override
    public InternalCompletableFuture<E> getAndSetAsync(E newValue) {
        Operation operation = new GetAndSetOperation(name, toData(newValue))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public boolean isNull() {
        return isNullAsync().join();
    }

    @Override
    public InternalCompletableFuture<Boolean> isNullAsync() {
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
