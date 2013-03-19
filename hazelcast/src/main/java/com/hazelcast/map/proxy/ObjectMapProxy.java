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

package com.hazelcast.map.proxy;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.*;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.Response;
import com.hazelcast.util.QueryResultStream;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.MapService.SERVICE_NAME;

public class ObjectMapProxy<K, V> extends MapProxySupport implements MapProxy<K, V> {

    public ObjectMapProxy(final String name, final MapService mapService, final NodeEngine nodeEngine) {
        super(name, mapService, nodeEngine);
    }

    public V get(Object k) {
        Data key = getService().toData(k);
        return (V) getService().toObject(getInternal(key));
    }

    public V put(final K k, final V v) {
        return put(k, v, -1, null);
    }

    public V put(final K k, final V v, final long ttl, final TimeUnit timeunit) {
        final Data key = getService().toData(k);
        final Data value = getService().toData(v);
        final Data result = putInternal(key, value, ttl, timeunit);
        return (V) getService().toObject(result);
    }

    public boolean tryPut(final K k, final V v, final long timeout, final TimeUnit timeunit) {
        final Data key = getService().toData(k);
        final Data value = getService().toData(v);
        return tryPutInternal(key, value, timeout, timeunit);
    }

    public V putIfAbsent(final K k, final V v) {
        return putIfAbsent(k, v, -1, null);
    }

    public V putIfAbsent(final K k, final V v, final long ttl, final TimeUnit timeunit) {
        final Data key = getService().toData(k);
        final Data value = getService().toData(v);
        final Data result = putIfAbsentInternal(key, value, ttl, timeunit);
        return (V) getService().toObject(result);
    }

    public void putTransient(final K k, final V v, final long ttl, final TimeUnit timeunit) {
        final Data key = getService().toData(k);
        final Data value = getService().toData(v);
        putTransientInternal(key, value, ttl, timeunit);
    }

    public boolean replace(final K k, final V o, final V v) {
        final Data key = getService().toData(k);
        final Data oldValue = getService().toData(o);
        final Data value = getService().toData(v);
        return replaceInternal(key, oldValue, value);
    }

    public V replace(final K k, final V v) {
        final Data key = getService().toData(k);
        final Data value = getService().toData(v);
        return (V) getService().toObject(replaceInternal(key, value));
    }

    public void set(K key, V value) {
        set(key, value, -1, TimeUnit.MILLISECONDS);
    }

    public void set(final K k, final V v, final long ttl, final TimeUnit timeunit) {
        final Data key = getService().toData(k);
        final Data value = getService().toData(v);
        setInternal(key, value, ttl, timeunit);
    }

    public V remove(Object k) {
        final Data key = getService().toData(k);
        final Data result = removeInternal(key);
        return (V) getService().toObject(result);
    }

    public boolean remove(final Object k, final Object v) {
        final Data key = getService().toData(k);
        final Data value = getService().toData(v);
        return removeInternal(key, value);
    }

    public void delete(Object k) {
        final Data key = getService().toData(k);
        deleteInternal(key);
    }

    public boolean containsKey(Object k) {
        Data key = getService().toData(k);
        return containsKeyInternal(key);
    }

    public boolean containsValue(final Object v) {
        Data value = getService().toData(v);
        return containsValueInternal(value);
    }

    public void lock(final K key) {
        NodeEngine nodeEngine = getNodeEngine();
        Data k = getService().toData(key);
        lockSupport.lock(nodeEngine, k);
    }

    public void unlock(final K key) {
        NodeEngine nodeEngine = getNodeEngine();
        Data k = getService().toData(key);
        lockSupport.unlock(nodeEngine, k);
    }

    public boolean tryRemove(final K key, final long timeout, final TimeUnit timeunit) {
        Data k = getService().toData(key);
        return tryRemoveInternal(k, timeout, timeunit);
    }

    public Future<V> getAsync(final K k) {
        Data key = getService().toData(k);
        NodeEngine nodeEngine = getNodeEngine();
        return new ObjectFuture(getAsyncInternal(key), nodeEngine.getSerializationService());
    }

    public boolean isLocked(final K k) {
        Data key = getService().toData(k);
        NodeEngine nodeEngine = getNodeEngine();
        return lockSupport.isLocked(nodeEngine, key);
    }

    public Future putAsync(final K key, final V value) {
        Data k = getService().toData(key);
        Data v = getService().toData(value);
        return new ObjectFuture(putAsyncInternal(k, v), getNodeEngine().getSerializationService());
    }

    public Future removeAsync(final K key) {
        Data k = getService().toData(key);
        return new ObjectFuture(removeAsyncInternal(k), getNodeEngine().getSerializationService());
    }

    public Map<K, V> getAll(final Set<K> keys) {
        Set<Data> ks = new HashSet(keys.size());
        for (K key : keys) {
            Data k = getService().toData(key);
            ks.add(k);
        }
        return (Map<K, V>) getAllObjectInternal(ks);
    }

    public void putAll(final Map<? extends K, ? extends V> m) {
        putAllObjectInternal(m);
    }

    public boolean tryLock(final K key) {
        final NodeEngine nodeEngine = getNodeEngine();
        return lockSupport.tryLock(nodeEngine, getService().toData(key));
    }

    public boolean tryLock(final K key, final long time, final TimeUnit timeunit) throws InterruptedException {
        final NodeEngine nodeEngine = getNodeEngine();
        return lockSupport.tryLock(nodeEngine, getService().toData(key), time, timeunit);
    }

    public void forceUnlock(final K key) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data k = getService().toData(key);
        lockSupport.forceUnlock(nodeEngine, k);
    }

    public void addInterceptor(MapInterceptor interceptor) {
        addMapInterceptorInternal(interceptor);
    }

    public void removeInterceptor(MapInterceptor interceptor) {
        removeMapInterceptorInternal(interceptor);
    }

    public void addEntryListener(final EntryListener listener, final boolean includeValue) {
        addEntryListenerInternal(listener, null, includeValue);
    }

    public void addEntryListener(final EntryListener<K, V> listener, final K key, final boolean includeValue) {
        addEntryListenerInternal(listener, getService().toData(key), includeValue);
    }

    public void addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        addEntryListenerInternal(listener, predicate, getService().toData(key), includeValue);
    }

    public void removeEntryListener(final EntryListener<K, V> listener) {
        removeEntryListenerInternal(listener);
    }

    public void removeEntryListener(final EntryListener<K, V> listener, final K key) {
        removeEntryListenerInternal(listener, getService().toData(key));
    }

    public EntryView<K, V> getEntryView(K key) {
        SimpleEntryView<K, V> entryViewInternal = (SimpleEntryView) getEntryViewInternal(getService().toData(key));
        Data value = (Data) entryViewInternal.getValue();
        entryViewInternal.setKey(key);
        entryViewInternal.setValue((V) getService().toObject(value));
        return entryViewInternal;
    }

    public boolean evict(final Object key) {
        return evictInternal(getService().toData(key));
    }

    public void clear() {
        clearInternal();
    }

    public Set<K> keySet() {
        final NodeEngine nodeEngine = getNodeEngine();
        Set<Data> dataSet = keySetInternal();
        HashSet<K> keySet = new HashSet<K>();
        for (Data data : dataSet) {
            keySet.add((K) getService().toObject(data));
        }
        return keySet;
    }

    public Collection<V> values() {
        final NodeEngine nodeEngine = getNodeEngine();
        Collection<Data> dataSet = valuesInternal();
        Collection<V> valueSet = new ArrayList<V>();
        for (Data data : dataSet) {
            valueSet.add((V) getService().toObject(data));
        }
        return valueSet;
    }

    public Set entrySet() {
        Set<Entry<Data, Data>> entries = entrySetInternal();
        Set<Entry<K, V>> resultSet = new HashSet<Entry<K, V>>();
        for (Entry<Data, Data> entry : entries) {
            resultSet.add(new AbstractMap.SimpleImmutableEntry((K) getService().toObject(entry.getKey()), (V) getService().toObject(entry.getValue())));
        }
        return resultSet;
    }

    public Set<K> keySet(final Predicate predicate) {
        return query(predicate, QueryResultStream.IterationType.KEY, false);
    }

    public Set entrySet(final Predicate predicate) {
        return query(predicate, QueryResultStream.IterationType.ENTRY, false);
    }

    public Collection<V> values(final Predicate predicate) {
        return query(predicate, QueryResultStream.IterationType.VALUE, false);
    }

    public Set<K> localKeySet() {
        final Set<Data> dataSet = localKeySetInternal();
        final Set<K> keySet = new HashSet<K>(dataSet.size());
        for (Data data : dataSet) {
            keySet.add((K) getService().toObject(data));
        }
        return keySet;
    }

    public Set<K> localKeySet(final Predicate predicate) {
        // todo implement
        return null;
    }

    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        return getService().toObject(executeOnKeyInternal(getService().toData(key), entryProcessor));
    }

    protected Object invoke(Operation operation, int partitionId) throws Throwable {
        NodeEngine nodeEngine = getNodeEngine();
        Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, operation, partitionId).build();
        Future f = invocation.invoke();
        Object response = f.get();
        Object returnObj;
        if (response instanceof Response) {
            Response r = (Response) response;
            returnObj = r.getResult();
        } else {
            returnObj = getService().toObject(response);
        }
        if (returnObj instanceof Throwable) {
            throw (Throwable) returnObj;
        }
        return returnObj;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("IMap");
        sb.append("{name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
