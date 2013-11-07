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
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapService;
import com.hazelcast.map.SimpleEntryView;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.executor.DelegatingFuture;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.MapService.SERVICE_NAME;

/** @author enesakar 1/17/13 */
public class MapProxyImpl<K, V> extends MapProxySupport implements IMap<K, V>, InitializingObject {

    public MapProxyImpl(final String name, final MapService mapService, final NodeEngine nodeEngine) {
        super(name, mapService, nodeEngine);
    }

    public V get(Object k) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        MapService service = getService();
        Data key = service.toData(k, partitionStrategy);
        return (V) service.toObject(getInternal(key));
    }

    public V put(final K k, final V v) {
        return put(k, v, -1, null);
    }

    public V put(final K k, final V v, final long ttl, final TimeUnit timeunit) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (v == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        MapService service = getService();
        final Data key = service.toData(k, partitionStrategy);
        final Data value = service.toData(v);
        final Data result = putInternal(key, value, ttl, timeunit);
        return (V) service.toObject(result);
    }

    public boolean tryPut(final K k, final V v, final long timeout, final TimeUnit timeunit) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (v == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        MapService service = getService();
        final Data key = service.toData(k, partitionStrategy);
        final Data value = service.toData(v);
        return tryPutInternal(key, value, timeout, timeunit);
    }

    public V putIfAbsent(final K k, final V v) {
        return putIfAbsent(k, v, -1, null);
    }

    public V putIfAbsent(final K k, final V v, final long ttl, final TimeUnit timeunit) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (v == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        MapService service = getService();
        final Data key = service.toData(k, partitionStrategy);
        final Data value = service.toData(v);
        final Data result = putIfAbsentInternal(key, value, ttl, timeunit);
        return (V) service.toObject(result);
    }

    public void putTransient(final K k, final V v, final long ttl, final TimeUnit timeunit) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (v == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        MapService service = getService();
        final Data key = service.toData(k, partitionStrategy);
        final Data value = service.toData(v);
        putTransientInternal(key, value, ttl, timeunit);
    }

    public boolean replace(final K k, final V o, final V v) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (o == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        if (v == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        MapService service = getService();
        final Data key = service.toData(k, partitionStrategy);
        final Data oldValue = service.toData(o);
        final Data value = service.toData(v);
        return replaceInternal(key, oldValue, value);
    }

    public V replace(final K k, final V v) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (v == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        MapService service = getService();
        final Data key = service.toData(k, partitionStrategy);
        final Data value = service.toData(v);
        return (V) service.toObject(replaceInternal(key, value));
    }

    public void set(K key, V value) {
        set(key, value, -1, TimeUnit.MILLISECONDS);
    }

    public void set(final K k, final V v, final long ttl, final TimeUnit timeunit) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (v == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        MapService service = getService();
        final Data key = service.toData(k, partitionStrategy);
        final Data value = service.toData(v);
        setInternal(key, value, ttl, timeunit);
    }

    public V remove(Object k) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        MapService service = getService();
        final Data key = service.toData(k, partitionStrategy);
        final Data result = removeInternal(key);
        return (V) service.toObject(result);
    }

    public boolean remove(final Object k, final Object v) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (v == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        MapService service = getService();
        final Data key = service.toData(k, partitionStrategy);
        final Data value = service.toData(v);
        return removeInternal(key, value);
    }

    public void delete(Object k) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final Data key = getService().toData(k, partitionStrategy);
        deleteInternal(key);
    }

    public boolean containsKey(Object k) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        Data key = getService().toData(k, partitionStrategy);
        return containsKeyInternal(key);
    }

    public boolean containsValue(final Object v) {
        if (v == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        Data value = getService().toData(v);
        return containsValueInternal(value);
    }

    public void lock(final K key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        NodeEngine nodeEngine = getNodeEngine();
        Data k = getService().toData(key, partitionStrategy);
        lockSupport.lock(nodeEngine, k);
    }

    public void lock(final Object key, final long leaseTime, final TimeUnit timeUnit) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        Data k = getService().toData(key, partitionStrategy);
        lockSupport.lock(getNodeEngine(), k, timeUnit.toMillis(leaseTime));
    }

    public void unlock(final K key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        NodeEngine nodeEngine = getNodeEngine();
        Data k = getService().toData(key, partitionStrategy);
        lockSupport.unlock(nodeEngine, k);
    }

    public boolean tryRemove(final K key, final long timeout, final TimeUnit timeunit) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        Data k = getService().toData(key, partitionStrategy);
        return tryRemoveInternal(k, timeout, timeunit);
    }

    public Future<V> getAsync(final K k) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        Data key = getService().toData(k, partitionStrategy);
        NodeEngine nodeEngine = getNodeEngine();
        return new DelegatingFuture<V>(getAsyncInternal(key), nodeEngine.getSerializationService());
    }

    public boolean isLocked(final K k) {
        if (k == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        Data key = getService().toData(k, partitionStrategy);
        NodeEngine nodeEngine = getNodeEngine();
        return lockSupport.isLocked(nodeEngine, key);
    }

    public Future putAsync(final K key, final V value) {
        return putAsync(key, value, -1, null);
    }

    public Future putAsync(final K key, final V value, final long ttl, final TimeUnit timeunit) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        MapService service = getService();
        Data k = service.toData(key, partitionStrategy);
        Data v = service.toData(value);
        return new DelegatingFuture<V>(putAsyncInternal(k, v, ttl, timeunit),
                                       getNodeEngine().getSerializationService());
    }

    public Future removeAsync(final K key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        Data k = getService().toData(key, partitionStrategy);
        return new DelegatingFuture<V>(removeAsyncInternal(k), getNodeEngine().getSerializationService());
    }

    public Map<K, V> getAll(final Set<K> keys) {
        Set<Data> ks = new HashSet(keys.size());
        MapService service = getService();
        for (K key : keys) {
            if (key == null) {
                throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
            }
            Data k = service.toData(key, partitionStrategy);
            ks.add(k);
        }
        return (Map<K, V>) getAllObjectInternal(ks);
    }

    public void putAll(final Map<? extends K, ? extends V> m) {
        // Note, putAllInternal() will take care of the null key/value checks.
        putAllInternal(m);
    }

    public boolean tryLock(final K key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final NodeEngine nodeEngine = getNodeEngine();
        Data k = getService().toData(key, partitionStrategy);
        return lockSupport.tryLock(nodeEngine, k);
    }

    public boolean tryLock(final K key, final long time, final TimeUnit timeunit) throws InterruptedException {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final NodeEngine nodeEngine = getNodeEngine();
        Data k = getService().toData(key, partitionStrategy);
        return lockSupport.tryLock(nodeEngine, k, time, timeunit);
    }

    public void forceUnlock(final K key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final NodeEngine nodeEngine = getNodeEngine();
        Data k = getService().toData(key, partitionStrategy);
        lockSupport.forceUnlock(nodeEngine, k);
    }

    public String addInterceptor(MapInterceptor interceptor) {
        if (interceptor == null) {
            throw new NullPointerException("Interceptor should not be null!");
        }
        return addMapInterceptorInternal(interceptor);
    }

    public void removeInterceptor(String id) {
        if (id == null) {
            throw new NullPointerException("Interceptor id should not be null!");
        }
        removeMapInterceptorInternal(id);
    }

    public String addEntryListener(final EntryListener listener, final boolean includeValue) {
        if (listener == null) {
            throw new NullPointerException("Listener should not be null!");
        }
        return addEntryListenerInternal(listener, null, includeValue);
    }

    public String addEntryListener(final EntryListener<K, V> listener, final K key, final boolean includeValue) {
        if (listener == null) {
            throw new NullPointerException("Listener should not be null!");
        }
        return addEntryListenerInternal(listener, getService().toData(key, partitionStrategy), includeValue);
    }

    public String addEntryListener(
            EntryListener<K, V> listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        if (listener == null) {
            throw new NullPointerException("Listener should not be null!");
        }
        if (predicate == null) {
            throw new NullPointerException("Predicate should not be null!");
        }
        return addEntryListenerInternal(listener, predicate, getService().toData(key, partitionStrategy), includeValue);
    }

    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, boolean includeValue) {
        if (listener == null) {
            throw new NullPointerException("Listener should not be null!");
        }
        if (predicate == null) {
            throw new NullPointerException("Predicate should not be null!");
        }
        return addEntryListenerInternal(listener, predicate, null, includeValue);
    }

    public boolean removeEntryListener(String id) {
        if (id == null) {
            throw new NullPointerException("Listener id should not be null!");
        }
        return removeEntryListenerInternal(id);
    }

    public EntryView<K, V> getEntryView(K key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        SimpleEntryView<K, V> entryViewInternal =
                (SimpleEntryView) getEntryViewInternal(getService().toData(key, partitionStrategy));
        if (entryViewInternal == null) {
            return null;
        }
        Data value = (Data) entryViewInternal.getValue();
        entryViewInternal.setKey(key);
        entryViewInternal.setValue((V) getService().toObject(value));
        return entryViewInternal;
    }

    public boolean evict(final Object key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        return evictInternal(getService().toData(key, partitionStrategy));
    }

    public void clear() {
        clearInternal();
    }

    public Set<K> keySet() {
        Set<Data> dataSet = keySetInternal();
        HashSet<K> keySet = new HashSet<K>();
        for (Data data : dataSet) {
            keySet.add((K) getService().toObject(data));
        }
        return keySet;
    }

    public Collection<V> values() {
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
            resultSet.add(new AbstractMap.SimpleImmutableEntry((K) getService().toObject(entry.getKey()),
                                                               (V) getService().toObject(entry.getValue())));
        }
        return resultSet;
    }

    public Set<K> keySet(final Predicate predicate) {
        if (predicate == null) {
            throw new NullPointerException("Predicate should not be null!");
        }
        return query(predicate, IterationType.KEY, false);
    }

    public Set entrySet(final Predicate predicate) {
        if (predicate == null) {
            throw new NullPointerException("Predicate should not be null!");
        }
        return query(predicate, IterationType.ENTRY, false);
    }

    public Collection<V> values(final Predicate predicate) {
        if (predicate == null) {
            throw new NullPointerException("Predicate should not be null!");
        }
        return query(predicate, IterationType.VALUE, false);
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
        if (predicate == null) {
            throw new NullPointerException("Predicate should not be null!");
        }
        return queryLocal(predicate, IterationType.KEY, false);
    }

    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        MapService service = getService();
        return service.toObject(executeOnKeyInternal(service.toData(key, partitionStrategy), entryProcessor));
    }

    protected Object invoke(Operation operation, int partitionId) throws Throwable {
        NodeEngine nodeEngine = getNodeEngine();
        Invocation invocation =
                nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, operation, partitionId).build();
        Future f = invocation.invoke();
        Object response = f.get();
        Object returnObj = getService().toObject(response);
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
