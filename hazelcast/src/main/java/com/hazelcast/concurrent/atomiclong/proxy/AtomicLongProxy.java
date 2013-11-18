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
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

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

    public long addAndGet(long delta) {
        try {
            AddAndGetOperation operation = new AddAndGetOperation(name, delta);
            Invocation inv = getNodeEngine().getOperationService().createInvocationBuilder(AtomicLongService.SERVICE_NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (Long) f.get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    public boolean compareAndSet(long expect, long update) {
        try {
            CompareAndSetOperation operation = new CompareAndSetOperation(name, expect, update);
            Invocation inv = getNodeEngine().getOperationService().createInvocationBuilder(AtomicLongService.SERVICE_NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (Boolean) f.get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    public void set(long newValue) {
        try {
            SetOperation operation = new SetOperation(name, newValue);
            Invocation inv = getNodeEngine().getOperationService().createInvocationBuilder(AtomicLongService.SERVICE_NAME, operation, partitionId).build();
            inv.invoke().get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    public long getAndSet(long newValue) {
        try {
            GetAndSetOperation operation = new GetAndSetOperation(name, newValue);
            Invocation inv = getNodeEngine().getOperationService().createInvocationBuilder(AtomicLongService.SERVICE_NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (Long) f.get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    public long getAndAdd(long delta) {
        try {
            GetAndAddOperation operation = new GetAndAddOperation(name, delta);
            Invocation inv = getNodeEngine().getOperationService().createInvocationBuilder(AtomicLongService.SERVICE_NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (Long) f.get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    public long decrementAndGet() {
        return addAndGet(-1);
    }

    public long get() {
        return getAndAdd(0);
    }

    public long incrementAndGet() {
        return addAndGet(1);
    }

    public long getAndIncrement() {
        return getAndAdd(1);
    }

    public String getName() {
        return name;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getServiceName() {
        return AtomicLongService.SERVICE_NAME;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IAtomicLong{");
        sb.append("name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
