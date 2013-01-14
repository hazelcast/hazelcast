/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.atomicnumber.proxy;

import com.hazelcast.atomicnumber.*;
import com.hazelcast.core.AtomicNumber;
import com.hazelcast.monitor.LocalAtomicNumberStats;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ServiceProxy;

import java.util.concurrent.Future;

// author: sancar - 21.12.2012
public class AtomicNumberProxy implements ServiceProxy, AtomicNumber {

    private final String name;
    private final NodeEngine nodeEngine;
    private final AtomicNumberService atomicNumberService;
    private final int partitionId;

    public AtomicNumberProxy(String name, AtomicNumberService atomicNumberService, NodeEngine nodeEngine) {

        this.name = name;
        this.nodeEngine = nodeEngine;
        this.atomicNumberService = atomicNumberService;
        this.partitionId = nodeEngine.getPartitionId(nodeEngine.toData(name));
    }

    public String getName() {
        return name;
    }

    public long addAndGet(long delta) {

        try {
            AddAndGetOperation operation = new AddAndGetOperation(name, delta);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(AtomicNumberService.NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (Long) f.get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public boolean compareAndSet(long expect, long update) {
        try {
            CompareAndSetOperation operation = new CompareAndSetOperation(name, expect, update);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(AtomicNumberService.NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (Boolean) f.get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public long decrementAndGet() {
        return addAndGet(-1);
    }

    public long incrementAndGet() {
        return addAndGet(1);
    }

    public long get() {
        return addAndGet(0);
    }

    public long getAndAdd(long delta) {
        try {
            GetAndAddOperation operation = new GetAndAddOperation(name, delta);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(AtomicNumberService.NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (Long) f.get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public long getAndSet(long newValue) {
        try {
            GetAndSetOperation operation = new GetAndSetOperation(name, newValue);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(AtomicNumberService.NAME, operation, partitionId).build();
            Future f = inv.invoke();
            return (Long) f.get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public void set(long newValue) {
        try {
            SetOperation operation = new SetOperation(name, newValue);
            Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(AtomicNumberService.NAME, operation, partitionId).build();
            inv.invoke();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    @Deprecated
    public boolean weakCompareAndSet(long expect, long update) {
        return false;
    }

    @Deprecated
    public void lazySet(long newValue) {

    }

    public LocalAtomicNumberStats getLocalAtomicNumberStats() {
        return null;
    }

    public void destroy() {

    }

    public Object getId() {
        return name;
    }

    public int getPartitionId() {
        return partitionId;
    }

}
