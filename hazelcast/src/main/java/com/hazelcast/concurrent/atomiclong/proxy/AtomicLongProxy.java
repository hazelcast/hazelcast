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
import com.hazelcast.concurrent.atomiclong.AlterAndGetOperation;
import com.hazelcast.concurrent.atomiclong.AlterOperation;
import com.hazelcast.concurrent.atomiclong.ApplyOperation;
import com.hazelcast.concurrent.atomiclong.CompareAndSetOperation;
import com.hazelcast.concurrent.atomiclong.GetAndAlterOperation;
import com.hazelcast.concurrent.atomiclong.GetAndSetOperation;
import com.hazelcast.concurrent.atomiclong.GetOperation;
import com.hazelcast.concurrent.atomiclong.SetOperation;
import com.hazelcast.core.Function;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * User: sancar
 * Date: 2/26/13
 * Time: 12:22 PM
 */
public class AtomicLongProxy extends AbstractDistributedObject<AtomicLongService> implements IAtomicLong {

    private final String name;
    private final int partitionId;

    public AtomicLongProxy(String name, NodeEngine nodeEngine, AtomicLongService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    private <E> E invoke(Operation operation, NodeEngine nodeEngine) {
        try {
            Invocation inv = nodeEngine
                    .getOperationService()
                    .createInvocationBuilder(AtomicLongService.SERVICE_NAME, operation, partitionId)
                    .build();
            Future<Data> f = inv.invoke();
            return nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public long addAndGet(long delta) {
        Operation operation = new AddAndGetOperation(name, delta);
        return (Long)invoke(operation,getNodeEngine());
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        Operation operation = new CompareAndSetOperation(name, expect, update);
        return (Boolean)invoke(operation,getNodeEngine());
    }

    @Override
    public void set(long newValue) {
        Operation operation = new SetOperation(name, newValue);
        invoke(operation,getNodeEngine());
    }

    @Override
    public long getAndSet(long newValue) {
        Operation operation = new GetAndSetOperation(name, newValue);
        return (Long) invoke(operation,getNodeEngine());
    }

    @Override
    public long getAndAdd(long delta) {
        Operation operation = new GetAndAddOperation(name, delta);
        return (Long)invoke(operation,getNodeEngine());
    }

    @Override
    public long decrementAndGet() {
        return addAndGet(-1);
    }

    @Override
    public long get() {
        GetOperation operation = new GetOperation(name);
        return (Long)invoke(operation,getNodeEngine());
    }

    @Override
    public long incrementAndGet() {
        return addAndGet(1);
    }

    @Override
    public long getAndIncrement() {
        return getAndAdd(1);
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
    public void alter(Function<Long, Long> function) {
        isNotNull(function,"function");

        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new AlterOperation(name, nodeEngine.toData(function));
        invoke(operation,nodeEngine);
    }

    @Override
    public long alterAndGet(Function<Long, Long> function) {
        isNotNull(function,"function");

        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new AlterAndGetOperation(name, nodeEngine.toData(function));
        return (Long)invoke(operation,nodeEngine);
    }

    @Override
    public long getAndAlter(Function<Long, Long> function) {
        isNotNull(function,"function");

        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new GetAndAlterOperation(name, nodeEngine.toData(function));
        return (Long)invoke(operation,nodeEngine);
    }

    @Override
    public <R> R apply(Function<Long, R> function) {
        isNotNull(function,"function");

        NodeEngine nodeEngine = getNodeEngine();
        Operation operation = new ApplyOperation(name, nodeEngine.toData(function));
        return invoke(operation,nodeEngine);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IAtomicLong{");
        sb.append("name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
