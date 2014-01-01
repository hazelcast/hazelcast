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

package com.hazelcast.concurrent.atomicreference.proxy;

import com.hazelcast.concurrent.atomicreference.*;
import com.hazelcast.core.AsyncAtomicReference;
import com.hazelcast.core.CompletableFuture;
import com.hazelcast.core.Function;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.ValidationUtil.isNotNull;

public class AtomicReferenceProxy<E> extends AbstractDistributedObject<AtomicReferenceService>
        implements AsyncAtomicReference<E> {

    private final String name;
    private final int partitionId;

    public AtomicReferenceProxy(String name, NodeEngine nodeEngine, AtomicReferenceService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    private <E> InternalCompletableFuture<E> asyncInvoke(Operation operation, NodeEngine nodeEngine) {
        try {
            OperationService operationService = nodeEngine.getOperationService();
            return operationService.invokeOnPartition(AtomicReferenceService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable throwable) {
            throw rethrow(throwable);
        }
    }

    @Override
    public void alter(Function<E, E> function) {
        asyncAlter(function).getSafely();
    }

    @Override
    public InternalCompletableFuture<Void> asyncAlter(Function<E, E> function) {
        isNotNull(function, "function");

        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new AlterOperation(name, nodeEngine.toData(function));
        return asyncInvoke(operation, nodeEngine);
    }

    @Override
    public E alterAndGet(Function<E, E> function) {
        return asyncAlterAndGet(function).getSafely();
    }

    @Override
    public InternalCompletableFuture<E> asyncAlterAndGet(Function<E, E> function) {
        isNotNull(function,"function");

        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new AlterAndGetOperation(name, nodeEngine.toData(function));
        return asyncInvoke(operation, nodeEngine);
    }

    @Override
    public E getAndAlter(Function<E, E> function) {
        return asyncGetAndAlter(function).getSafely();
    }

    @Override
    public InternalCompletableFuture<E> asyncGetAndAlter(Function<E, E> function) {
        isNotNull(function,"function");

        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new GetAndAlterOperation(name, nodeEngine.toData(function));
        return asyncInvoke(operation, nodeEngine);
    }

    @Override
    public <R> R apply(Function<E, R> function) {
        return asyncApply(function).getSafely();
    }

    @Override
    public <R> InternalCompletableFuture<R> asyncApply(Function<E, R> function) {
        isNotNull(function,"function");

        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new ApplyOperation(name, nodeEngine.toData(function));
        return asyncInvoke(operation, nodeEngine);
    }

    @Override
    public void clear() {
        set(null);
    }

    @Override
    public InternalCompletableFuture<Void> asyncClear() {
        return asyncSet(null);
    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        return asyncCompareAndSet(expect,update).getSafely();
    }

    @Override
    public InternalCompletableFuture<Boolean> asyncCompareAndSet(E expect, E update) {
        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new CompareAndSetOperation(name, nodeEngine.toData(expect), nodeEngine.toData(update));
        return asyncInvoke(operation,nodeEngine);
    }

    @Override
    public E get() {
        return asyncGet().getSafely();
    }

    @Override
    public InternalCompletableFuture<E> asyncGet() {
        Operation operation = new GetOperation(name);
        return asyncInvoke(operation, getNodeEngine());
    }

    @Override
    public boolean contains(E expected) {
        return asyncContains(expected).getSafely();
    }

    @Override
    public InternalCompletableFuture<Boolean> asyncContains(E value) {
        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new ContainsOperation(name,nodeEngine.toData(value));
        return asyncInvoke(operation,nodeEngine);
    }

    @Override
    public void set(E newValue) {
        asyncSet(newValue).getSafely();
    }

    @Override
    public InternalCompletableFuture<Void> asyncSet(E newValue) {
        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new SetOperation(name, nodeEngine.toData(newValue));
        return asyncInvoke(operation, nodeEngine);
    }

    @Override
    public E getAndSet(E newValue) {
        return asyncGetAndSet(newValue).getSafely();
    }

    @Override
    public InternalCompletableFuture<E> asyncGetAndSet(E newValue) {
        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new GetAndSetOperation(name, nodeEngine.toData(newValue));
        return asyncInvoke(operation, nodeEngine);
    }

    @Override
    public E setAndGet(E update) {
        return asyncSetAndGet(update).getSafely();
    }

    @Override
    public InternalCompletableFuture<E> asyncSetAndGet(E update) {
        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new SetAndGetOperation(name, nodeEngine.toData(update));
        return asyncInvoke(operation, nodeEngine);
    }

    @Override
    public boolean isNull() {
        return asyncIsNull().getSafely();
    }

    @Override
    public InternalCompletableFuture<Boolean> asyncIsNull() {
        Operation operation = new IsNullOperation(name);
        return asyncInvoke(operation,getNodeEngine());
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
        final StringBuilder sb = new StringBuilder("IAtomicReference{");
        sb.append("name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
