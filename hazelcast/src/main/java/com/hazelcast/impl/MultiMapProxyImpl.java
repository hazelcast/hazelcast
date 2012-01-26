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

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.ConcurrentMapManager.MMultiGet;
import com.hazelcast.impl.ConcurrentMapManager.MRemoveMulti;
import com.hazelcast.impl.base.FactoryAwareNamedProxy;
import com.hazelcast.nio.DataSerializable;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MultiMapProxyImpl extends FactoryAwareNamedProxy implements MultiMapProxy, DataSerializable, IGetAwareProxy {

    private transient MultiMapProxy base = null;

    public MultiMapProxyImpl() {
    }

    MultiMapProxyImpl(String name, FactoryImpl factory) {
        setName(name);
        setHazelcastInstance(factory);
        this.base = new MultiMapReal();
    }

    private void ensure() {
        factory.initialChecks();
        if (base == null) {
            base = (MultiMapProxy) factory.getOrCreateProxyByName(name);
        }
    }

    public MultiMapReal getBase() {
        return (MultiMapReal) base;
    }

    public MProxy getMProxy() {
        ensure();
        return base.getMProxy();
    }

    public Object getId() {
        ensure();
        return base.getId();
    }

    @Override
    public String toString() {
        return "MultiMap [" + getName() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiMapProxyImpl that = (MultiMapProxyImpl) o;
        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public InstanceType getInstanceType() {
        ensure();
        return base.getInstanceType();
    }

    public void destroy() {
        factory.destroyInstanceClusterWide(name, null);
    }

    public String getName() {
        ensure();
        return base.getName();
    }

    public boolean put(Object key, Object value) {
        ensure();
        return base.put(key, value);
    }

    public Collection get(Object key) {
        ensure();
        return base.get(key);
    }

    public boolean remove(Object key, Object value) {
        ensure();
        return base.remove(key, value);
    }

    public Collection remove(Object key) {
        ensure();
        return base.remove(key);
    }

    public Set localKeySet() {
        ensure();
        return base.localKeySet();
    }

    public Set keySet() {
        ensure();
        return base.keySet();
    }

    public Collection values() {
        ensure();
        return base.values();
    }

    public Set entrySet() {
        ensure();
        return base.entrySet();
    }

    public boolean containsKey(Object key) {
        ensure();
        return base.containsKey(key);
    }

    public boolean containsValue(Object value) {
        ensure();
        return base.containsValue(value);
    }

    public boolean containsEntry(Object key, Object value) {
        ensure();
        return base.containsEntry(key, value);
    }

    public int size() {
        ensure();
        return base.size();
    }

    public void clear() {
        ensure();
        base.clear();
    }

    public int valueCount(Object key) {
        ensure();
        return base.valueCount(key);
    }

    public void addLocalEntryListener(EntryListener entryListener) {
        ensure();
        base.addLocalEntryListener(entryListener);
    }

    public void addEntryListener(EntryListener entryListener, boolean includeValue) {
        ensure();
        base.addEntryListener(entryListener, includeValue);
    }

    public void removeEntryListener(EntryListener entryListener) {
        ensure();
        base.removeEntryListener(entryListener);
    }

    public void addEntryListener(EntryListener entryListener, Object key, boolean includeValue) {
        ensure();
        base.addEntryListener(entryListener, key, includeValue);
    }

    public void removeEntryListener(EntryListener entryListener, Object key) {
        ensure();
        base.removeEntryListener(entryListener, key);
    }

    public void lock(Object key) {
        ensure();
        base.lock(key);
    }

    public boolean tryLock(Object key) {
        ensure();
        return base.tryLock(key);
    }

    public boolean tryLock(Object key, long time, TimeUnit timeunit) {
        ensure();
        return base.tryLock(key, time, timeunit);
    }

    public void unlock(Object key) {
        ensure();
        base.unlock(key);
    }

    public boolean lockMap(long time, TimeUnit timeunit) {
        ensure();
        return base.lockMap(time, timeunit);
    }

    public void unlockMap() {
        ensure();
        base.unlockMap();
    }

    class MultiMapReal implements MultiMapProxy, IGetAwareProxy {
        final MProxy mapProxy;

        private MultiMapReal() {
            mapProxy = new MProxyImpl(name, factory);
        }

        public MProxy getMProxy() {
            return mapProxy;
        }

        public String getName() {
            return name.substring(Prefix.MULTIMAP.length());
        }

        public void clear() {
            mapProxy.clear();
        }

        public boolean containsEntry(Object key, Object value) {
            return mapProxy.containsEntry(key, value);
        }

        public boolean containsKey(Object key) {
            return mapProxy.containsKey(key);
        }

        public boolean containsValue(Object value) {
            return mapProxy.containsValue(value);
        }

        public Collection get(Object key) {
            MMultiGet multiGet = factory.node.concurrentMapManager.new MMultiGet();
            return multiGet.get(name, key);
        }

        public boolean put(Object key, Object value) {
            return mapProxy.putMulti(key, value);
        }

        public boolean remove(Object key, Object value) {
            return mapProxy.removeMulti(key, value);
        }

        public Collection remove(Object key) {
            MRemoveMulti m = factory.node.concurrentMapManager.new MRemoveMulti();
            return m.remove(name, key);
        }

        public int size() {
            return mapProxy.size();
        }

        public Set localKeySet() {
            return mapProxy.localKeySet();
        }

        public Set keySet() {
            return mapProxy.keySet();
        }

        public Collection values() {
            return mapProxy.values();
        }

        public Set entrySet() {
            return mapProxy.entrySet();
        }

        public int valueCount(Object key) {
            return mapProxy.valueCount(key);
        }

        public InstanceType getInstanceType() {
            return InstanceType.MULTIMAP;
        }

        public void destroy() {
            mapProxy.destroy();
        }

        public Object getId() {
            return name;
        }

        public void addLocalEntryListener(EntryListener entryListener) {
            mapProxy.addLocalEntryListener(entryListener);
        }

        public void addEntryListener(EntryListener entryListener, boolean includeValue) {
            mapProxy.addEntryListener(entryListener, includeValue);
        }

        public void removeEntryListener(EntryListener entryListener) {
            mapProxy.removeEntryListener(entryListener);
        }

        public void addEntryListener(EntryListener entryListener, Object key, boolean includeValue) {
            mapProxy.addEntryListener(entryListener, key, includeValue);
        }

        public void removeEntryListener(EntryListener entryListener, Object key) {
            mapProxy.removeEntryListener(entryListener, key);
        }

        public void lock(Object key) {
            mapProxy.lock(key);
        }

        public boolean tryLock(Object key) {
            return mapProxy.tryLock(key);
        }

        public boolean tryLock(Object key, long time, TimeUnit timeunit) {
            return mapProxy.tryLock(key, time, timeunit);
        }

        public void unlock(Object key) {
            mapProxy.unlock(key);
        }

        public boolean lockMap(long time, TimeUnit timeunit) {
            return mapProxy.lockMap(time, timeunit);
        }

        public void unlockMap() {
            mapProxy.unlockMap();
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        }
    }
}
