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

package com.hazelcast.impl;

import com.hazelcast.core.*;
import com.hazelcast.impl.base.FactoryAwareNamedProxy;
import com.hazelcast.impl.monitor.CountDownLatchOperationsCounter;
import com.hazelcast.impl.monitor.LocalCountDownLatchStatsImpl;
import com.hazelcast.monitor.LocalCountDownLatchStats;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.nio.IOUtil.toData;

public class CountDownLatchProxyImpl extends FactoryAwareNamedProxy implements CountDownLatchProxy {
    private transient CountDownLatchProxy base = null;
    Data nameAsData = null;

    public CountDownLatchProxyImpl(String name, FactoryImpl factory) {
        set(name, factory);
        base = new CountDownLatchProxyReal();
    }

    public void set(String name, FactoryImpl factory) {
        setName(name);
        setHazelcastInstance(factory);
    }

    public InstanceType getInstanceType() {
        ensure();
        return base.getInstanceType();
    }

    public void destroy() {
        ensure();
        base.destroy();
    }

    public Object getId() {
        ensure();
        return base.getId();
    }

    public String getName() {
        ensure();
        return base.getName();
    }

    public String getLongName() {
        return name;
    }

    @Override
    public String toString() {
        return "CountDownLatch [" + getName() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        CountDownLatchProxyImpl that = (CountDownLatchProxyImpl) o;
        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode() {
        int result = base != null ? base.hashCode() : 0;
        result = 31 * result + (nameAsData != null ? nameAsData.hashCode() : 0);
        return result;
    }

    public void await() throws InstanceDestroyedException, MemberLeftException, InterruptedException {
        ensure();
        base.await();
    }

    public boolean await(long timeout, TimeUnit unit) throws InstanceDestroyedException, MemberLeftException, InterruptedException {
        ensure();
        return base.await(timeout, unit);
    }

    public void countDown() {
        ensure();
        base.countDown();
    }

    public int getCount() {
        ensure();
        return base.getCount();
    }

    public Member getOwner() {
        ensure();
        return base.getOwner();
    }

    public boolean hasCount() {
        ensure();
        return base.hasCount();
    }

    public boolean setCount(int count) {
        ensure();
        return base.setCount(count);
    }

    public boolean setCount(int count, Address ownerAddress) {
        ensure();
        return base.setCount(count, ownerAddress);
    }

    public LocalCountDownLatchStats getLocalCountDownLatchStats() {
        ensure();
        return base.getLocalCountDownLatchStats();
    }

    public CountDownLatchOperationsCounter getCountDownLatchOperationsCounter() {
        ensure();
        return base.getCountDownLatchOperationsCounter();
    }

    private Data getNameAsData() {
        if (nameAsData == null) {
            nameAsData = toData(name);
        }
        return nameAsData;
    }

    private void ensure() {
        factory.initialChecks();
        if (base == null) {
            base = (CountDownLatchProxy) factory.getOrCreateProxyByName(name);
        }
    }

    private class CountDownLatchProxyReal implements CountDownLatchProxy {
        CountDownLatchOperationsCounter operationsCounter = new CountDownLatchOperationsCounter();

        public CountDownLatchProxyReal() {
        }

        public String getName() {
            return name.substring(Prefix.COUNT_DOWN_LATCH.length());
        }

        public String getLongName() {
            return name;
        }

        public Object getId() {
            return name;
        }

        public void await() throws InstanceDestroyedException, MemberLeftException, InterruptedException {
            await(-1, TimeUnit.MILLISECONDS);
        }

        public boolean await(long timeout, TimeUnit unit) throws InstanceDestroyedException, MemberLeftException, InterruptedException {
            if (Thread.interrupted())
                throw new InterruptedException();
            return newMCountDownLatch().await(getNameAsData(), timeout, unit);
        }

        public void countDown() {
            newMCountDownLatch().countDown(getNameAsData());
        }

        public void destroy() {
            newMCountDownLatch().destroy(getNameAsData());
            factory.destroyInstanceClusterWide(name, null);
        }

        public int getCount() {
            return newMCountDownLatch().getCount(getNameAsData());
        }

        public Member getOwner() {
            final Address owner = newMCountDownLatch().getOwnerAddress(getNameAsData());
            final Address local = factory.node.baseVariables.thisAddress;
            return owner != null ? new MemberImpl(owner, local.equals(owner)) : null;
        }

        public boolean hasCount() {
            return newMCountDownLatch().getCount(getNameAsData()) > 0;
        }

        public boolean setCount(int count) {
            return setCount(count, factory.node.getThisAddress());
        }

        public boolean setCount(int count, Address ownerAddress) {
            return newMCountDownLatch().setCount(getNameAsData(), count, ownerAddress);
        }

        public InstanceType getInstanceType() {
            return InstanceType.COUNT_DOWN_LATCH;
        }

        public CountDownLatchOperationsCounter getCountDownLatchOperationsCounter() {
            return operationsCounter;
        }

        public LocalCountDownLatchStats getLocalCountDownLatchStats() {
            LocalCountDownLatchStatsImpl localCountDownLatchStats = new LocalCountDownLatchStatsImpl();
            localCountDownLatchStats.setOperationStats(operationsCounter.getPublishedStats());
            return localCountDownLatchStats;
        }

        ConcurrentMapManager.MCountDownLatch newMCountDownLatch() {
            ConcurrentMapManager.MCountDownLatch mcdl = factory.node.concurrentMapManager.new MCountDownLatch();
            mcdl.setOperationsCounter(operationsCounter);
            return mcdl;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        }
    }
}
