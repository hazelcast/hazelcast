/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
import com.hazelcast.impl.ConcurrentMapManager.*;
import com.hazelcast.impl.base.FactoryAwareNamedProxy;
import com.hazelcast.impl.concurrentmap.AddMapIndex;
import com.hazelcast.impl.map.MapProxy;
import com.hazelcast.impl.monitor.LocalMapStatsImpl;
import com.hazelcast.impl.monitor.MapOperationsCounter;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.query.Expression;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.util.Clock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.impl.ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS;
import static com.hazelcast.impl.Util.toMillis;
import static com.hazelcast.nio.IOUtil.toData;

public class MProxyImpl extends FactoryAwareNamedProxy implements MProxy, DataSerializable {

    private transient MProxy mproxyReal = null;

    private transient ConcurrentMapManager concurrentMapManager = null;

    private transient ListenerManager listenerManager = null;

    private volatile transient MProxy dynamicProxy;

    public MProxyImpl() {
    }

    class DynamicInvoker implements InvocationHandler {
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            beforeCall();
            try {
                return method.invoke(mproxyReal, args);
            } catch (Throwable e) {
                if (e instanceof InvocationTargetException) {
                    InvocationTargetException ite = (InvocationTargetException) e;
                    throw ite.getCause();
                }
                throw e;
            } finally {
                afterCall();
            }
        }
    }

    MProxyImpl(String name, FactoryImpl factory) {
        setName(name);
        setHazelcastInstance(factory);
        mproxyReal = new MProxyReal();
    }

    public MapOperationsCounter getMapOperationCounter() {
        return mproxyReal.getMapOperationCounter();
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        super.setHazelcastInstance(hazelcastInstance);
        this.concurrentMapManager = factory.node.concurrentMapManager;
        this.listenerManager = factory.node.listenerManager;
        ClassLoader cl = MProxy.class.getClassLoader();
        dynamicProxy = (MProxy) Proxy.newProxyInstance(cl, new Class[]{MProxy.class}, new DynamicInvoker());
    }

    private void beforeCall() {
        factory.initialChecks();
        if (mproxyReal == null) {
            mproxyReal = (MProxy) factory.getOrCreateProxyByName(name);
        }
    }

    private void afterCall() {
    }

    public Object get(Object key) {
        beforeCall();
        try {
            return mproxyReal.get(key);
        } catch (Throwable e) {
            Util.throwUncheckedException(e);
            return null;
        } finally {
            afterCall();
        }
    }

    public Object put(Object key, Object value) {
        return put(key, value, 0, TimeUnit.SECONDS);
    }

    public Future getAsync(Object key) {
        beforeCall();
        final MProxyImpl mProxy = MProxyImpl.this;
        final Data dataKey = toData(key);
        AsyncCall call = new AsyncCall() {
            @Override
            protected void call() {
                setResult(mProxy.get(dataKey));
            }
        };
        factory.node.executorManager.executeAsync(call);
        return call;
    }

    public Future putAsync(Object key, Object value) {
        beforeCall();
        final MProxyImpl mProxy = MProxyImpl.this;
        final Data dataKey = toData(key);
        final Data dataValue = toData(value);
        AsyncCall call = new AsyncCall() {
            @Override
            protected void call() {
                setResult(mProxy.put(dataKey, dataValue));
            }
        };
        factory.node.executorManager.executeAsync(call);
        return call;
    }

    public Future removeAsync(Object key) {
        beforeCall();
        final MProxyImpl mProxy = MProxyImpl.this;
        final Data dataKey = toData(key);
        AsyncCall call = new AsyncCall() {
            @Override
            protected void call() {
                setResult(mProxy.remove(dataKey));
            }
        };
        factory.node.executorManager.executeAsync(call);
        return call;
    }

    public Object put(Object key, Object value, long ttl, TimeUnit timeunit) {
        beforeCall();
        try {
            return mproxyReal.put(key, value, ttl, timeunit);
        } catch (Throwable e) {
            Util.throwUncheckedException(e);
            return null;
        } finally {
            afterCall();
        }
    }

    public Object remove(Object key) {
        beforeCall();
        try {
            return mproxyReal.remove(key);
        } catch (Throwable e) {
            Util.throwUncheckedException(e);
            return null;
        } finally {
            afterCall();
        }
    }

    public Object tryRemove(Object key, long time, TimeUnit timeunit) throws TimeoutException {
        beforeCall();
        try {
            return mproxyReal.tryRemove(key, time, timeunit);
        } catch (Throwable e) {
            if (e instanceof TimeoutException) {
                throw (TimeoutException) e;
            }
            Util.throwUncheckedException(e);
            return null;
        } finally {
            afterCall();
        }
    }

    public void putAndUnlock(Object key, Object value) {
        dynamicProxy.putAndUnlock(key, value);
    }

    public Object tryLockAndGet(Object key, long time, TimeUnit timeunit) throws TimeoutException {
        return dynamicProxy.tryLockAndGet(key, time, timeunit);
    }

    public Map getAll(Set keys) {
        return dynamicProxy.getAll(keys);
    }

    public void flush() {
        dynamicProxy.flush();
    }

    public void putForSync(Object key, Object value) {
        dynamicProxy.putForSync(key, value);
    }

    public void removeForSync(Object key) {
        dynamicProxy.removeForSync(key);
    }

    public void putTransient(Object key, Object value, long time, TimeUnit timeunit) {
        dynamicProxy.putTransient(key, value, time, timeunit);
    }

    public boolean tryPut(Object key, Object value, long time, TimeUnit timeunit) {
        return dynamicProxy.tryPut(key, value, time, timeunit);
    }

    public void set(Object key, Object value, long time, TimeUnit timeunit) {
        dynamicProxy.set(key, value, time, timeunit);
    }

    public Object putIfAbsent(Object key, Object value, long ttl, TimeUnit timeunit) {
        return dynamicProxy.putIfAbsent(key, value, ttl, timeunit);
    }

    public Object putIfAbsent(Object key, Object value) {
        return dynamicProxy.putIfAbsent(key, value);
    }

    public LocalMapStats getLocalMapStats() {
        return dynamicProxy.getLocalMapStats();
    }

    public void addIndex(String attribute, boolean ordered) {
        dynamicProxy.addIndex(attribute, ordered);
    }

    public void addIndex(Expression expression, boolean ordered) {
        dynamicProxy.addIndex(expression, ordered);
    }

    public Object getId() {
        return dynamicProxy.getId();
    }

    @Override
    public String toString() {
        return "Map [" + getName() + "] ";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MProxyImpl mProxy = (MProxyImpl) o;
        return name.equals(mProxy.name);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public void destroy() {
        dynamicProxy.destroy();
    }

    public InstanceType getInstanceType() {
        return dynamicProxy.getInstanceType();
    }

    public boolean removeKey(Object key) {
        return dynamicProxy.removeKey(key);
    }

    public int size() {
        return dynamicProxy.size();
    }

    public boolean isEmpty() {
        return dynamicProxy.isEmpty();
    }

    public boolean containsKey(Object key) {
        return dynamicProxy.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return dynamicProxy.containsValue(value);
    }

    public MapEntry getMapEntry(Object key) {
        return dynamicProxy.getMapEntry(key);
    }

    public void putAll(Map t) {
        dynamicProxy.putAll(t);
    }

    public void clear() {
        dynamicProxy.clear();
    }

    public int valueCount(Object key) {
        return dynamicProxy.valueCount(key);
    }

    public Set allKeys() {
        return dynamicProxy.allKeys();
    }

    public Set localKeySet() {
        return dynamicProxy.localKeySet();
    }

    public Set localKeySet(Predicate predicate) {
        return dynamicProxy.localKeySet(predicate);
    }

    public Set keySet() {
        return dynamicProxy.keySet();
    }

    public Collection values() {
        return dynamicProxy.values();
    }

    public Set entrySet() {
        return dynamicProxy.entrySet();
    }

    public Set keySet(Predicate predicate) {
        return dynamicProxy.keySet(predicate);
    }

    public Collection values(Predicate predicate) {
        return dynamicProxy.values(predicate);
    }

    public Set entrySet(Predicate predicate) {
        return dynamicProxy.entrySet(predicate);
    }

    public boolean remove(Object key, Object value) {
        return dynamicProxy.remove(key, value);
    }

    public boolean replace(Object key, Object oldValue, Object newValue) {
        return dynamicProxy.replace(key, oldValue, newValue);
    }

    public Object replace(Object key, Object value) {
        return dynamicProxy.replace(key, value);
    }

    public String getName() {
        return name.substring(Prefix.MAP.length());
    }

    public boolean lockMap(long time, TimeUnit timeunit) {
        return dynamicProxy.lockMap(time, timeunit);
    }

    public void unlockMap() {
        dynamicProxy.unlockMap();
    }

    public void lock(Object key) {
        dynamicProxy.lock(key);
    }

    public boolean isLocked(Object key) {
        return dynamicProxy.isLocked(key);
    }

    public boolean tryLock(Object key) {
        return dynamicProxy.tryLock(key);
    }

    public boolean tryLock(Object key, long time, TimeUnit timeunit) {
        return dynamicProxy.tryLock(key, time, timeunit);
    }

    public void unlock(Object key) {
        dynamicProxy.unlock(key);
    }

    public void forceUnlock(Object key) {
        dynamicProxy.forceUnlock(key);
    }

    public String getLongName() {
        return dynamicProxy.getLongName();
    }

    public void addGenericListener(Object listener, Object key, boolean includeValue,
                                   InstanceType instanceType) {
        dynamicProxy.addGenericListener(listener, key, includeValue, instanceType);
    }

    public void removeGenericListener(Object listener, Object key) {
        dynamicProxy.removeGenericListener(listener, key);
    }

    public void addLocalEntryListener(EntryListener entryListener) {
        dynamicProxy.addLocalEntryListener(entryListener);
    }

    public void addEntryListener(EntryListener listener, boolean includeValue) {
        dynamicProxy.addEntryListener(listener, includeValue);
    }

    public void addEntryListener(EntryListener listener, Object key, boolean includeValue) {
        dynamicProxy.addEntryListener(listener, key, includeValue);
    }

    public void removeEntryListener(EntryListener listener) {
        dynamicProxy.removeEntryListener(listener);
    }

    public void removeEntryListener(EntryListener listener, Object key) {
        dynamicProxy.removeEntryListener(listener, key);
    }

    public boolean containsEntry(Object key, Object value) {
        return dynamicProxy.containsEntry(key, value);
    }

    public boolean putMulti(Object key, Object value) {
        return dynamicProxy.putMulti(key, value);
    }

    public boolean removeMulti(Object key, Object value) {
        return dynamicProxy.removeMulti(key, value);
    }

    public boolean add(Object value) {
        return dynamicProxy.add(value);
    }

    public boolean evict(Object key) {
        return dynamicProxy.evict(key);
    }

    private static void check(Object obj) {
        Util.checkSerializable(obj);
    }

    private class MProxyReal implements MProxy {
        private final transient MapOperationsCounter mapOperationCounter = new MapOperationsCounter();
        private final MapProxy mapProxy;

        public MProxyReal() {
            super();
            mapProxy = new MapProxy(factory.node.nodeService);
        }

        @Override
        public String toString() {
            return MProxyImpl.this.toString();
        }

        public InstanceType getInstanceType() {
            return InstanceType.MAP;
        }

        public Object getId() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            return MProxyImpl.this.equals(o);
        }

        @Override
        public int hashCode() {
            return MProxyImpl.this.hashCode();
        }

        public String getLongName() {
            return name;
        }

        public String getName() {
            return MProxyImpl.this.getName();
        }

        public void addIndex(final String attribute, final boolean ordered) {
            addIndex(Predicates.get(attribute), ordered);
        }

        public void addIndex(final Expression expression, final boolean ordered) {
            final CountDownLatch latch = new CountDownLatch(1);
            concurrentMapManager.enqueueAndReturn(new Processable() {
                public void process() {
                    AddMapIndex addMapIndexProcess = new AddMapIndex(name, expression, ordered);
                    concurrentMapManager.sendProcessableToAll(addMapIndexProcess, true);
                    latch.countDown();
                }
            });
            try {
                latch.await();
            } catch (InterruptedException ignored) {
            }
        }

        public void flush() {
            concurrentMapManager.flush(name);
        }

        public MapEntry getMapEntry(Object key) {
            long begin = Clock.currentTimeMillis();
            check(key);
            MGetMapEntry mgetMapEntry = concurrentMapManager.new MGetMapEntry();
            MapEntry mapEntry = mgetMapEntry.get(name, key);
            mapOperationCounter.incrementGets(Clock.currentTimeMillis() - begin);
            return mapEntry;
        }

        public boolean putMulti(Object key, Object value) {
            check(key);
            check(value);
            MPutMulti mput = concurrentMapManager.new MPutMulti();
            return mput.put(name, key, value);
        }

        public Object put(Object key, Object value) {
            return put(key, value, 0, TimeUnit.SECONDS);
        }

        public void putForSync(Object key, Object value) {
            long begin = Clock.currentTimeMillis();
            check(key);
            check(value);
            MPut mput = concurrentMapManager.new MPut();
            mput.putForSync(name, key, value);
            mput.clearRequest();
            mapOperationCounter.incrementPuts(Clock.currentTimeMillis() - begin);
        }

        public void removeForSync(Object key) {
            long begin = Clock.currentTimeMillis();
            check(key);
            MRemove mremove = concurrentMapManager.new MRemove();
            mremove.removeForSync(name, key);
            mremove.clearRequest();
            mapOperationCounter.incrementRemoves(Clock.currentTimeMillis() - begin);
        }

        public Map getAll(Set keys) {
            if (keys == null) {
                throw new NullPointerException();
            }
            return concurrentMapManager.getAll(name, keys);
        }

        public Future getAsync(Object key) {
            throw new UnsupportedOperationException();
        }

        public Future putAsync(Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        public Future removeAsync(Object key) {
            throw new UnsupportedOperationException();
        }

        public Object put(Object key, Object value, long ttl, TimeUnit timeunit) {
            if (ttl < 0) {
                throw new IllegalArgumentException("ttl value cannot be negative. " + ttl);
            }
            if (ttl == 0) {
                ttl = -1;
            } else {
                ttl = toMillis(ttl, timeunit);
            }
            return put(key, value, ttl);
        }

        public void putTransient(Object key, Object value, long ttl, TimeUnit timeunit) {
            if (ttl < 0) {
                throw new IllegalArgumentException("ttl value cannot be negative. " + ttl);
            }
            if (ttl == 0) {
                ttl = -1;
            } else {
                ttl = toMillis(ttl, timeunit);
            }
            mapOperationCounter.incrementOtherOperations();
            concurrentMapManager.putTransient(name, key, value, ttl);
        }

        public Object put(Object key, Object value, long ttl) {
            long begin = Clock.currentTimeMillis();
            check(key);
            check(value);
//            MPut mput = ThreadContext.get().getCallCache(factory).getMPut();
//            Object result = mput.put(name, key, value, timeout, ttl);
            Object result = mapProxy.put(name, key, value, ttl);
//            mput.clearRequest();
            mapOperationCounter.incrementPuts(Clock.currentTimeMillis() - begin);
            return result;
        }

        public void set(Object key, Object value, long ttl, TimeUnit timeunit) {
            long begin = Clock.currentTimeMillis();
            if (ttl < 0) {
                throw new IllegalArgumentException("ttl value cannot be negative. " + ttl);
            }
            if (ttl == 0) {
                ttl = -1;
            } else {
                ttl = toMillis(ttl, timeunit);
            }
            check(key);
            check(value);
            MPut mput = concurrentMapManager.new MPut();
            mput.set(name, key, value, ttl);
            mput.clearRequest();
            mapOperationCounter.incrementPuts(Clock.currentTimeMillis() - begin);
        }

        public boolean tryPut(Object key, Object value, long timeout, TimeUnit timeunit) {
            long begin = Clock.currentTimeMillis();
            if (timeout < 0) {
                throw new IllegalArgumentException("timeout value cannot be negative. " + timeout);
            }
            timeout = toMillis(timeout, timeunit);
            check(key);
            check(value);
            MPut mput = concurrentMapManager.new MPut();
            Boolean result = mput.tryPut(name, key, value, timeout, -1);
            mput.clearRequest();
            mapOperationCounter.incrementPuts(Clock.currentTimeMillis() - begin);
            return result;
        }

        public Object tryLockAndGet(Object key, long timeout, TimeUnit timeunit) throws TimeoutException {
            long begin = Clock.currentTimeMillis();
            if (timeout < 0) {
                throw new IllegalArgumentException("timeout value cannot be negative. " + timeout);
            }
            timeout = toMillis(timeout, timeunit);
            check(key);
            Object result = concurrentMapManager.tryLockAndGet(name, key, timeout);
            mapOperationCounter.incrementGets(Clock.currentTimeMillis() - begin);
            return result;
        }

        public boolean lockMap(long time, TimeUnit timeunit) {
            if (factory.locksMapProxy.tryLock("map_lock_" + name, time, timeunit)) {
                MLockMap mLockMap = concurrentMapManager.new MLockMap(name, true);
                mLockMap.call();
                return true;
            }
            return false;
        }

        public void unlockMap() {
            MLockMap mLockMap = concurrentMapManager.new MLockMap(name, false);
            mLockMap.call();
            factory.locksMapProxy.unlock("map_lock_" + name);
        }

        public void lock(Object key) {
            check(key);
            mapOperationCounter.incrementOtherOperations();
            concurrentMapManager.lock(name, key, -1);
        }

        public boolean isLocked(Object key) {
            check(key);
            mapOperationCounter.incrementOtherOperations();
            MLock mlock = concurrentMapManager.new MLock();
            return mlock.isLocked(name, key);
        }

        public boolean tryLock(Object key) {
            check(key);
            mapOperationCounter.incrementOtherOperations();
            return concurrentMapManager.lock(name, key, 0);
        }

        public boolean tryLock(Object key, long time, TimeUnit timeunit) {
            check(key);
            if (time < 0)
                throw new IllegalArgumentException("Time cannot be negative. time = " + time);
            mapOperationCounter.incrementOtherOperations();
            long timeoutMillis = toMillis(time, timeunit);
            return concurrentMapManager.lock(name, key, timeoutMillis);
        }

        public void unlock(Object key) {
            check(key);
            mapOperationCounter.incrementOtherOperations();
            MLock mlock = concurrentMapManager.new MLock();
            if (!mlock.unlock(name, key, 0)) {
                throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
            }
        }

        public void forceUnlock(Object key) {
            check(key);
            mapOperationCounter.incrementOtherOperations();
            MLock mlock = concurrentMapManager.new MLock();
            mlock.forceUnlock(name, key);
        }

        public void putAndUnlock(Object key, Object value) {
            long begin = Clock.currentTimeMillis();
            check(key);
            check(value);
            concurrentMapManager.putAndUnlock(name, key, value);
            mapOperationCounter.incrementPuts(Clock.currentTimeMillis() - begin);
        }

        public Object putIfAbsent(Object key, Object value) {
            return putIfAbsent(key, value, -1);
        }

        public Object putIfAbsent(Object key, Object value, long ttl, TimeUnit timeunit) {
            if (ttl < 0) {
                throw new IllegalArgumentException("ttl value cannot be negative. " + ttl);
            }
            if (ttl == 0) {
                ttl = -1;
            } else {
                ttl = toMillis(ttl, timeunit);
            }
            return putIfAbsent(key, value, ttl);
        }

        private Object putIfAbsent(Object key, Object value, long ttl) {
            long begin = Clock.currentTimeMillis();
            check(key);
            check(value);
            MPut mput = concurrentMapManager.new MPut();
            Object result = mput.putIfAbsent(name, key, value, ttl);
            mput.clearRequest();
            mapOperationCounter.incrementPuts(Clock.currentTimeMillis() - begin);
            return result;
        }

        public Object get(Object key) {
            check(key);
            long begin = Clock.currentTimeMillis();
//            MGet mget = ThreadContext.get().getCallCache(factory).getMGet();
//            Object result = mget.get(name, key, -1);
//            mget.clearRequest();
            Object result = mapProxy.getOperation(name, key);
            mapOperationCounter.incrementGets(Clock.currentTimeMillis() - begin);
            return result;
        }

        public Object remove(Object key) {
            long begin = Clock.currentTimeMillis();
            check(key);
            MRemove mremove = concurrentMapManager.new MRemove();
            Object result = mremove.remove(name, key);
            mremove.clearRequest();
            mapOperationCounter.incrementRemoves(Clock.currentTimeMillis() - begin);
            return result;
        }

        public Object tryRemove(Object key, long timeout, TimeUnit timeunit) throws TimeoutException {
            long begin = Clock.currentTimeMillis();
            check(key);
            MRemove mremove = concurrentMapManager.new MRemove();
            Object result = mremove.tryRemove(name, key, toMillis(timeout, timeunit));
            mremove.clearRequest();
            mapOperationCounter.incrementRemoves(Clock.currentTimeMillis() - begin);
            return result;
        }

        public int size() {
            mapOperationCounter.incrementOtherOperations();
            return mapProxy.getSize(name);
        }

        public int valueCount(Object key) {
            int count;
            mapOperationCounter.incrementOtherOperations();
            MValueCount mcount = concurrentMapManager.new MValueCount();
            count = ((Number) mcount.count(name, key, -1)).intValue();
            return count;
        }

        public boolean removeMulti(Object key, Object value) {
            check(key);
            check(value);
            MRemoveMulti mremove = concurrentMapManager.new MRemoveMulti();
            return mremove.remove(name, key, value);
        }

        public boolean remove(Object key, Object value) {
            long begin = Clock.currentTimeMillis();
            check(key);
            check(value);
            MRemove mremove = concurrentMapManager.new MRemove();
            boolean result = mremove.removeIfSame(name, key, value);
            mapOperationCounter.incrementRemoves(Clock.currentTimeMillis() - begin);
            return result;
        }

        public Object replace(Object key, Object value) {
            long begin = Clock.currentTimeMillis();
            check(key);
            check(value);
            MPut mput = concurrentMapManager.new MPut();
            Object result = mput.replace(name, key, value);
            mapOperationCounter.incrementPuts(Clock.currentTimeMillis() - begin);
            return result;
        }

        public boolean replace(Object key, Object oldValue, Object newValue) {
            long begin = Clock.currentTimeMillis();
            check(key);
            check(oldValue);
            check(newValue);
            MPut mput = concurrentMapManager.new MPut();
            Boolean result = mput.replace(name, key, oldValue, newValue);
            mapOperationCounter.incrementPuts(Clock.currentTimeMillis() - begin);
            return result;
        }

        public LocalMapStats getLocalMapStats() {
            mapOperationCounter.incrementOtherOperations();
            LocalMapStatsImpl localMapStats = concurrentMapManager.getLocalMapStats(name);
            localMapStats.setOperationStats(mapOperationCounter.getPublishedStats());
            return localMapStats;
        }

        public void addGenericListener(Object listener, Object key, boolean includeValue,
                                       InstanceType instanceType) {
            if (listener == null)
                throw new IllegalArgumentException("Listener cannot be null");
            listenerManager.addListener(name, listener, key, includeValue, instanceType);
        }

        public void removeGenericListener(Object listener, Object key) {
            if (listener == null)
                throw new IllegalArgumentException("Listener cannot be null");
            listenerManager.removeListener(name, listener, key);
        }

        public void addLocalEntryListener(EntryListener listener) {
            if (listener == null)
                throw new IllegalArgumentException("Listener cannot be null");
            listenerManager.addLocalListener(name, listener, getInstanceType());
        }

        public void addEntryListener(EntryListener listener, boolean includeValue) {
            if (listener == null)
                throw new IllegalArgumentException("Listener cannot be null");
            addGenericListener(listener, null, includeValue, getInstanceType());
        }

        public void addEntryListener(EntryListener listener, Object key, boolean includeValue) {
            if (listener == null)
                throw new IllegalArgumentException("Listener cannot be null");
            check(key);
            addGenericListener(listener, key, includeValue, getInstanceType());
        }

        public void removeEntryListener(EntryListener listener) {
            if (listener == null)
                throw new IllegalArgumentException("Listener cannot be null");
            removeGenericListener(listener, null);
        }

        public void removeEntryListener(EntryListener listener, Object key) {
            if (listener == null)
                throw new IllegalArgumentException("Listener cannot be null");
            check(key);
            removeGenericListener(listener, key);
        }

        public boolean containsEntry(Object key, Object value) {
            check(key);
            check(value);
            mapOperationCounter.incrementOtherOperations();
            TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
            if (txn != null && txn.has(name, key)) {
                if (txn.containsEntry(name, key, value)) {
                    return true;
                }
            }
            MContainsKey mContainsKey = concurrentMapManager.new MContainsKey();
            return mContainsKey.containsEntry(name, key, value);
        }

        public boolean containsKey(Object key) {
            check(key);
            mapOperationCounter.incrementOtherOperations();
            TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
            if (txn != null) {
                if (txn.has(name, key)) {
                    Data value = txn.get(name, key);
                    return value != null;
                }
            }
            MContainsKey mContainsKey = concurrentMapManager.new MContainsKey();
            return mContainsKey.containsKey(name, key);
        }

        public boolean containsValue(Object value) {
            check(value);
            mapOperationCounter.incrementOtherOperations();
            TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
            if (txn != null) {
                if (txn.containsValue(name, value))
                    return true;
            }
            MContainsValue mContainsValue = concurrentMapManager.new MContainsValue(name, value);
            return mContainsValue.call();
        }

        public boolean isEmpty() {
            mapOperationCounter.incrementOtherOperations();
            MEmpty mempty = concurrentMapManager.new MEmpty();
            return mempty.isEmpty(name);
        }

        public void putAll(Map map) {
            Set<Entry> entries = map.entrySet();
            TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                for (final Entry entry : entries) {
                    put(entry.getKey(), entry.getValue());
                }
            } else {
                concurrentMapManager.doPutAll(name, map);
            }
        }

        public boolean add(Object value) {
            check(value);
            Object old = putIfAbsent(value, toData(Boolean.TRUE));
            return old == null;
        }

        public boolean removeKey(Object key) {
            long begin = Clock.currentTimeMillis();
            check(key);
            MRemoveItem mRemoveItem = concurrentMapManager.new MRemoveItem();
            boolean result = mRemoveItem.removeItem(name, key);
            mapOperationCounter.incrementRemoves(Clock.currentTimeMillis() - begin);
            return result;
        }

        public void clear() {
            Set keys = keySet();
            for (Object key : keys) {
                removeKey(key);
            }
        }

        public Set localKeySet() {
            return localKeySet(null);
        }

        public Set localKeySet(Predicate predicate) {
            mapOperationCounter.incrementOtherOperations();
            return concurrentMapManager.queryLocal(name, CONCURRENT_MAP_ITERATE_KEYS, predicate);
        }

        public Set entrySet(Predicate predicate) {
            return (Set) query(ClusterOperation.CONCURRENT_MAP_ITERATE_ENTRIES, predicate);
        }

        public Set keySet(Predicate predicate) {
            return (Set) query(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS, predicate);
        }

        public Collection values(Predicate predicate) {
            return query(ClusterOperation.CONCURRENT_MAP_ITERATE_VALUES, predicate);
        }

        public Set entrySet() {
            return (Set) query(ClusterOperation.CONCURRENT_MAP_ITERATE_ENTRIES, null);
        }

        public Set keySet() {
            return (Set) query(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS, null);
        }

        public Set allKeys() {
            return (Set) query(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS_ALL, null);
        }

        public MapOperationsCounter getMapOperationCounter() {
            return mapOperationCounter;
        }

        public Collection values() {
            return query(ClusterOperation.CONCURRENT_MAP_ITERATE_VALUES, null);
        }

        private Collection query(ClusterOperation iteratorType, Predicate predicate) {
            mapOperationCounter.incrementOtherOperations();
            return concurrentMapManager.query(name, iteratorType, predicate);
        }

        public void destroy() {
            factory.destroyInstanceClusterWide(name, null);
        }

        public boolean evict(Object key) {
            mapOperationCounter.incrementOtherOperations();
            MEvict mevict = concurrentMapManager.new MEvict();
            boolean result = mevict.evict(name, key);
            mevict.clearRequest();
            return result;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        }
    }
}
