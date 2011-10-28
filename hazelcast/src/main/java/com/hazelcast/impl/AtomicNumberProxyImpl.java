/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import static com.hazelcast.nio.IOUtil.toData;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.ConcurrentMapManager.MAtomicNumber;
import com.hazelcast.impl.base.FactoryAwareNamedProxy;
import com.hazelcast.impl.monitor.AtomicNumberOperationsCounter;
import com.hazelcast.impl.monitor.LocalAtomicNumberStatsImpl;
import com.hazelcast.monitor.LocalAtomicNumberStats;
import com.hazelcast.nio.Data;

public class AtomicNumberProxyImpl extends FactoryAwareNamedProxy implements AtomicNumberProxy {
    private transient AtomicNumberProxy base = null;
    Data nameAsData = null;

    public AtomicNumberProxyImpl() {
    }

    AtomicNumberProxyImpl(String name, FactoryImpl factory) {
        setName(name);
        setHazelcastInstance(factory);
        base = new AtomicNumberProxyReal();
    }

    Data getNameAsData() {
        if (nameAsData == null) {
            nameAsData = toData(name);
        }
        return nameAsData;
    }

    private void ensure() {
        factory.initialChecks();
        if (base == null) {
            base = (AtomicNumberProxy) factory.getOrCreateProxyByName(name);
        }
    }

    @Override
    public String toString() {
        return "AtomicLong [" + getName() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AtomicNumberProxyImpl that = (AtomicNumberProxyImpl) o;
        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public void destroy() {
        ensure();
        base.destroy();
    }

    public InstanceType getInstanceType() {
        ensure();
        return base.getInstanceType();
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

    public long addAndGet(long delta) {
        ensure();
        return base.addAndGet(delta);
    }

    public boolean compareAndSet(long expect, long update) {
        ensure();
        return base.compareAndSet(expect, update);
    }

    public long decrementAndGet() {
        ensure();
        return base.decrementAndGet();
    }

    public long get() {
        ensure();
        return base.get();
    }

    public long getAndAdd(long delta) {
        ensure();
        return base.getAndAdd(delta);
    }

    public long getAndSet(long newValue) {
        ensure();
        return base.getAndSet(newValue);
    }

    public long incrementAndGet() {
        ensure();
        return base.incrementAndGet();
    }

    public void set(long newValue) {
        ensure();
        base.set(newValue);
    }

    public AtomicNumberOperationsCounter getOperationsCounter() {
        ensure();
        return base.getOperationsCounter();
    }

    public LocalAtomicNumberStats getLocalAtomicNumberStats() {
        ensure();
        return base.getLocalAtomicNumberStats();
    }

    @Deprecated
    public void lazySet(long newValue) {
        set(newValue);
    }

    @Deprecated
    public boolean weakCompareAndSet(long expect, long update) {
        return compareAndSet(expect, update);
    }

    private class AtomicNumberProxyReal implements AtomicNumberProxy {
        AtomicNumberOperationsCounter operationsCounter = new AtomicNumberOperationsCounter();

        public AtomicNumberProxyReal() {
        }

        public String getName() {
            return name.substring(Prefix.ATOMIC_NUMBER.length());
        }

        public String getLongName() {
            return name;
        }

        public Object getId() {
            return name;
        }

        public long addAndGet(long delta) {
            return newMAtomicNumber().addAndGet(getNameAsData(), delta);
        }

        public boolean compareAndSet(long expect, long update) {
            return newMAtomicNumber().compareAndSet(getNameAsData(), expect, update);
        }

        public long decrementAndGet() {
            return addAndGet(-1L);
        }

        public long get() {
            return addAndGet(0L);
        }

        public long getAndAdd(long delta) {
            return newMAtomicNumber().getAndAdd(getNameAsData(), delta);
        }

        public long getAndSet(long newValue) {
            return newMAtomicNumber().getAndSet(getNameAsData(), newValue);
        }

        public long incrementAndGet() {
            return addAndGet(1L);
        }

        public void set(long newValue) {
            getAndSet(newValue);
        }

        public InstanceType getInstanceType() {
            return InstanceType.ATOMIC_NUMBER;
        }

        public void destroy() {
            newMAtomicNumber().destroy(getNameAsData());
            factory.destroyInstanceClusterWide(name, null);
        }

        public AtomicNumberOperationsCounter getOperationsCounter() {
            return operationsCounter;
        }

        public LocalAtomicNumberStats getLocalAtomicNumberStats() {
            LocalAtomicNumberStatsImpl localAtomicStats = new LocalAtomicNumberStatsImpl();
            localAtomicStats.setOperationStats(operationsCounter.getPublishedStats());
            return localAtomicStats;
        }

        @Deprecated
        public void lazySet(long newValue) {
            set(newValue);
        }

        @Deprecated
        public boolean weakCompareAndSet(long expect, long update) {
            return compareAndSet(expect, update);
        }

        MAtomicNumber newMAtomicNumber() {
            MAtomicNumber mAtomicNumber = factory.node.concurrentMapManager.new MAtomicNumber();
            mAtomicNumber.setOperationsCounter(operationsCounter);
            return mAtomicNumber;
        }
        
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		}
    }
}