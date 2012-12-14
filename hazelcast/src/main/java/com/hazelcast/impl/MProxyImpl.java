///*
// * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.impl;
//
//import com.hazelcast.core.EntryListener;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.core.MapEntry;
//import com.hazelcast.core.Prefix;
//import com.hazelcast.impl.base.FactoryAwareNamedProxy;
//import com.hazelcast.instance.HazelcastInstanceImpl;
//import com.hazelcast.map.MapProxy;
//import com.hazelcast.map.MapService;
//import com.hazelcast.monitor.impl.MapOperationsCounter;
//import com.hazelcast.monitor.LocalMapStats;
//import com.hazelcast.query.Expression;
//import com.hazelcast.query.Predicate;
//import com.hazelcast.util.Clock;
//import com.hazelcast.util.Util;
//
//import java.util.Collection;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//
//import static com.hazelcast.nio.IOUtil.toData;
//import static com.hazelcast.util.Util.toMillis;
//
//public class MProxyImpl extends FactoryAwareNamedProxy implements MProxy {
//
//    private final MapProxy mapProxy;
//    private final MapOperationsCounter mapOperationCounter;
//
//    public MProxyImpl(String name, HazelcastInstanceImpl instance) {
//        setName(name);
//        setHazelcastInstance(instance);
//        final MapService mapService = instance.node.nodeEngine.getService(MapService.MAP_SERVICE_NAME);
//        mapProxy = mapService.getProxy(name);
//        mapOperationCounter = new MapOperationsCounter();
//    }
//
//    @Override
//    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
//        super.setHazelcastInstance(hazelcastInstance);
//    }
//
//    private void beforeCall() {
////        factory.initialChecks();
//    }
//
//    private void afterCall() {
//    }
//
//    @Override
//    public String toString() {
//        return "Map [" + getName() + "] ";
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        MProxyImpl mProxy = (MProxyImpl) o;
//        return name.equals(mProxy.name);
//    }
//
//    @Override
//    public int hashCode() {
//        return name != null ? name.hashCode() : 0;
//    }
//
//    public String getName() {
//        return name.substring(Prefix.MAP.length());
//    }
//
//    public Map getAll(final Set keys) {
//        return null;
//    }
//
//    private static void check(Object obj) {
//        Util.checkNotNull(obj);
//    }
//
//    public InstanceType getInstanceType() {
//        return InstanceType.MAP;
//    }
//
//    public Object getId() {
//        return name;
//    }
//
//    public String getLongName() {
//        return name;
//    }
//
//    public void addGenericListener(final Object listener, final Object key, final boolean includeValue, final InstanceType instanceType) {
//
//    }
//
//    public void removeGenericListener(final Object listener, final Object key) {
//
//    }
//
//    public boolean containsEntry(final Object key, final Object value) {
//        return false;
//    }
//
//    public boolean putFromLoad(final Object key, final Object value) {
//        return false;
//    }
//
//    public boolean putMulti(final Object key, final Object value) {
//        return false;
//    }
//
//    public boolean removeMulti(final Object key, final Object value) {
//        return false;
//    }
//
//    public LocalMapStats getLocalMapStats() {
//        return null;
//    }
//
//    public Object put(Object key, Object value) {
//        return put(key, value, 0, TimeUnit.SECONDS);
//    }
//
//    public Object remove(final Object key) {
//        long begin = Clock.currentTimeMillis();
//        check(key);
//        Object result = mapProxy.remove(key);
//        mapOperationCounter.incrementRemoves(Clock.currentTimeMillis() - begin);
//        return result;
//    }
//
//    public void putAll(final Map m) {
//
//    }
//
//    public boolean remove(final Object key, final Object value) {
//        return false;
//    }
//
//    public void flush() {
//
//    }
//
//    public Future getAsync(Object key) {
//        throw new UnsupportedOperationException();
//    }
//
//    public Future putAsync(Object key, Object value) {
//        throw new UnsupportedOperationException();
//    }
//
//    public Future removeAsync(Object key) {
//        throw new UnsupportedOperationException();
//    }
//
//    public Object tryRemove(final Object key, final long timeout, final TimeUnit timeunit) throws TimeoutException {
//        return null;
//    }
//
//    public boolean tryPut(final Object key, final Object value, final long timeout, final TimeUnit timeunit) {
//        return false;
//    }
//
//    public Object put(Object key, Object value, long ttl, TimeUnit timeunit) {
//        if (ttl < 0) {
//            throw new IllegalArgumentException("ttl value cannot be negative. " + ttl);
//        }
//        if (ttl == 0) {
//            ttl = -1;
//        } else {
//            ttl = toMillis(ttl, timeunit);
//        }
//        return put(key, value, ttl);
//    }
//
//    public void putTransient(final Object key, final Object value, long ttl, final TimeUnit timeunit) {
//        if (ttl < 0) {
//            throw new IllegalArgumentException("ttl value cannot be negative. " + ttl);
//        }
//        if (ttl == 0) {
//            ttl = -1;
//        } else {
//            ttl = toMillis(ttl, timeunit);
//        }
//
//        long begin = Clock.currentTimeMillis();
//        check(key);
//        check(value);
//        mapProxy.putTransient(name, key, value, ttl);
//        mapOperationCounter.incrementPuts(Clock.currentTimeMillis() - begin);
//
//    }
//
//    public Object putIfAbsent(final Object key, final Object value) {
//          return putIfAbsent(key, value, 0, TimeUnit.SECONDS);
//    }
//
//    public Object putIfAbsent(final Object key, final Object value, long ttl, final TimeUnit timeunit) {
//        if (ttl < 0) {
//            throw new IllegalArgumentException("ttl value cannot be negative. " + ttl);
//        }
//        if (ttl == 0) {
//            ttl = -1;
//        } else {
//            ttl = toMillis(ttl, timeunit);
//        }
//
//        long begin = Clock.currentTimeMillis();
//        check(key);
//        check(value);
//        Object res = mapProxy.putIfAbsent(name, key, value, ttl);
//        mapOperationCounter.incrementPuts(Clock.currentTimeMillis() - begin);
//        return res;
//    }
//
//    public boolean replace(final Object key, final Object oldValue, final Object newValue) {
//        return false;
//    }
//
//    public Object replace(final Object key, final Object value) {
//        return null;
//    }
//
//    public void set(final Object key, final Object value, long ttl, final TimeUnit timeunit) {
//        if (ttl < 0) {
//            throw new IllegalArgumentException("ttl value cannot be negative. " + ttl);
//        }
//        if (ttl == 0) {
//            ttl = -1;
//        } else {
//            ttl = toMillis(ttl, timeunit);
//        }
//
//        long begin = Clock.currentTimeMillis();
//        check(key);
//        check(value);
//        mapProxy.set(name, key, value, ttl);
//        // todo incrementPuts or incrementOthers ???
//        mapOperationCounter.incrementPuts(Clock.currentTimeMillis() - begin);
//
//    }
//
//    public Object tryLockAndGet(final Object key, final long time, final TimeUnit timeunit) throws TimeoutException {
//        return null;
//    }
//
//    public void putAndUnlock(final Object key, final Object value) {
//
//    }
//
//    public void lock(final Object key) {
//
//    }
//
//    public boolean isLocked(final Object key) {
//        return false;
//    }
//
//    public boolean tryLock(final Object key) {
//        return false;
//    }
//
//    public boolean tryLock(final Object key, final long time, final TimeUnit timeunit) {
//        return false;
//    }
//
//    public void unlock(final Object key) {
//
//    }
//
//    public void forceUnlock(final Object key) {
//
//    }
//
//    public boolean lockMap(final long time, final TimeUnit timeunit) {
//        return false;
//    }
//
//    public void unlockMap() {
//
//    }
//
//    public void addLocalEntryListener(final EntryListener listener) {
//
//    }
//
//    public void addEntryListener(final EntryListener listener, final boolean includeValue) {
//
//    }
//
//    public void removeEntryListener(final EntryListener listener) {
//
//    }
//
//    public void addEntryListener(final EntryListener listener, final Object key, final boolean includeValue) {
//
//    }
//
//    public void removeEntryListener(final EntryListener listener, final Object key) {
//
//    }
//
//    public MapEntry getMapEntry(final Object key) {
//        return null;
//    }
//
//    public boolean evict(final Object key) {
//        return false;
//    }
//
//    public Set keySet() {
//        return null;
//    }
//
//    public Collection values() {
//        return null;
//    }
//
//    public Set entrySet() {
//        return null;
//    }
//
//    public Set keySet(final Predicate predicate) {
//        return null;
//    }
//
//    public Set entrySet(final Predicate predicate) {
//        return null;
//    }
//
//    public Collection values(final Predicate predicate) {
//        return null;
//    }
//
//    public Object put(Object key, Object value, long ttl) {
//        long begin = Clock.currentTimeMillis();
//        check(key);
//        check(value);
////            MPut mput = ThreadContext.get().getCallCache(factory).getMPut();
////            Object result = mput.put(name, key, value, timeout, ttl);
//        Object result = mapProxy.put(name, key, value, ttl);
////            mput.clearRequest();
//        mapOperationCounter.incrementPuts(Clock.currentTimeMillis() - begin);
//        return result;
//    }
//
//    public boolean containsKey(final Object key) {
//        check(key);
//        long begin = Clock.currentTimeMillis();
//        Boolean result = (Boolean) mapProxy.containsKey(name, key);
//        mapOperationCounter.incrementGets(Clock.currentTimeMillis() - begin);
//        return result;
//    }
//
//    public boolean containsValue(final Object value) {
//        return false;
//    }
//
//    public Object get(Object key) {
//        check(key);
//        long begin = Clock.currentTimeMillis();
////            MGet mget = ThreadContext.get().getCallCache(factory).getMGet();
////            Object result = mget.get(name, key, -1);
////            mget.clearRequest();
//        Object result = mapProxy.get(name, key);
//        mapOperationCounter.incrementGets(Clock.currentTimeMillis() - begin);
//        return result;
//    }
//
//    public int size() {
//        mapOperationCounter.incrementOtherOperations();
//        return mapProxy.getSize(name);
//    }
//
//    public boolean isEmpty() {
//        return false;
//    }
//
//    public boolean add(Object value) {
//        check(value);
//        Object old = putIfAbsent(value, toData(Boolean.TRUE));
//        return old == null;
//    }
//
//    public int valueCount(final Object key) {
//        return 0;
//    }
//
//    public Set allKeys() {
//        return null;
//    }
//
//    public void clear() {
//        Set keys = keySet();
//        for (Object key : keys) {
//            removeKey(key);
//        }
//    }
//
//    public Set localKeySet() {
//        return localKeySet(null);
//    }
//
//    public Set localKeySet(final Predicate predicate) {
//        return null;
//    }
//
//    public void addIndex(final String attribute, final boolean ordered) {
//
//    }
//
//    public void addIndex(final Expression expression, final boolean ordered) {
//
//    }
//
//    public MapOperationsCounter getMapOperationCounter() {
//        return mapOperationCounter;
//    }
//
//    public void putForSync(final Object key, final Object value) {
//
//    }
//
//    public void removeForSync(final Object key) {
//
//    }
//
//    public void destroy() {
//        hazelcastInstance.destroyInstance(name);
//    }
//
//    public boolean removeKey(final Object key) {
//        return false;
//    }
//}
