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

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Instance;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.base.FactoryAwareNamedProxy;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import static com.hazelcast.nio.IOUtil.toData;

public class AtomicNumberImpl extends FactoryAwareNamedProxy implements AtomicNumber, DataSerializable {
    AtomicNumberReal base = null;
    Data nameAsData = null;

    public AtomicNumberImpl(String name, FactoryImpl factory) {
        setName(name);
        setHazelcastInstance(factory);
        base = new AtomicNumberReal();
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
            base = (AtomicNumberReal) factory.getOrCreateProxyByName(name);
        }
    }

    public Object getId() {
        ensure();
        return base.getId();
    }

    @Override
    public String toString() {
        return "IdGenerator [" + getName() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtomicNumberImpl that = (AtomicNumberImpl) o;
        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public Instance.InstanceType getInstanceType() {
        ensure();
        return base.getInstanceType();
    }

    public void destroy() {
        ensure();
        base.destroy();
    }

    public String getName() {
        ensure();
        return base.getName();
    }

    public long addAndGet(long delta) {
        ensure();
        return base.addAndGet(delta);
    }

    public boolean compareAndSet(long expect, long update) {
        ensure();
        return base.compareAndSet(expect, update);
    }

    public boolean weakCompareAndSet(long expect, long update) {
        ensure();
        return base.weakCompareAndSet(expect, update);
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

    public void lazySet(long newValue) {
        ensure();
        base.lazySet(newValue);
    }

    public void set(long newValue) {
        ensure();
        base.set(newValue);
    }

    private class AtomicNumberReal implements AtomicNumber {

        private AtomicNumberReal() {
        }

        public String getName() {
            return name.substring(Prefix.ATOMIC_NUMBER.length());
        }

        ConcurrentMapManager.MAtomic newMAtomic(ClusterOperation op, long value, long expected) {
            return factory.node.concurrentMapManager.new MAtomic(getNameAsData(), op, value, expected);
        }

        ConcurrentMapManager.MAtomic newMAtomic(ClusterOperation op, long value) {
            return factory.node.concurrentMapManager.new MAtomic(getNameAsData(), op, value);
        }

        public long addAndGet(long delta) {
            ConcurrentMapManager.MAtomic a = newMAtomic(ClusterOperation.ATOMIC_NUMBER_ADD_AND_GET, delta);
            long result = a.doLongAtomic();
            a.backup(result);
            return result;
        }

        public boolean compareAndSet(long expect, long update) {
            ConcurrentMapManager.MAtomic a = newMAtomic(ClusterOperation.ATOMIC_NUMBER_COMPARE_AND_SET, update, expect);
            boolean result = a.doBooleanAtomic();
            if (result) {
                a.backup(update);
            }
            return result;
        }

        public boolean weakCompareAndSet(long expect, long update) {
            return compareAndSet(expect, update);
        }

        public long decrementAndGet() {
            return addAndGet(-1L);
        }

        public long get() {
            return addAndGet(0L);
        }

        public long getAndAdd(long delta) {
            ConcurrentMapManager.MAtomic a = newMAtomic(ClusterOperation.ATOMIC_NUMBER_GET_AND_ADD, delta);
            long result = a.doLongAtomic();
            a.backup(result + delta);
            return result;
        }

        public long getAndSet(long newValue) {
            ConcurrentMapManager.MAtomic a = newMAtomic(ClusterOperation.ATOMIC_NUMBER_GET_AND_SET, newValue);
            long result = a.doLongAtomic();
            a.backup(newValue);
            return result;
        }

        public long incrementAndGet() {
            return addAndGet(1L);
        }

        public void lazySet(long newValue) {
            set(newValue);
        }

        public void set(long newValue) {
            getAndSet(newValue);
        }

        public InstanceType getInstanceType() {
            return InstanceType.ATOMIC_NUMBER;
        }

        public void destroy() {
            factory.node.concurrentMapManager.new MRemove().remove(FactoryImpl.ATOMIC_NUMBER_MAP_NAME, nameAsData, 0);
        }

        public Object getId() {
            return name;
        }
    }
}
