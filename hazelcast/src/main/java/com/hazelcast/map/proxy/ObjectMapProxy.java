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

package com.hazelcast.map.proxy;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEntry;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.Response;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.map.MapService.MAP_SERVICE_NAME;
import static com.hazelcast.nio.IOUtil.toObject;

public class ObjectMapProxy<K, V> extends MapProxySupport implements MapProxy<K, V> {

    public ObjectMapProxy(final String name, final MapService mapService, final NodeEngine nodeEngine) {
        super(name, mapService, nodeEngine);
    }

    public V get(Object k) {
        Data key = nodeEngine.toData(k);
        return toObject(getInternal(key));
    }

    public V put(final K k, final V v) {
        return put(k, v, -1, null);
    }

    public V put(final K k, final V v, final long ttl, final TimeUnit timeunit) {
        final Data key = nodeEngine.toData(k);
        final Data value = nodeEngine.toData(v);
        final Data result = putInternal(key, value, ttl, timeunit);
        return toObject(result);
    }

    public boolean tryPut(final K k, final V v, final long timeout, final TimeUnit timeunit) {
        final Data key = nodeEngine.toData(k);
        final Data value = nodeEngine.toData(v);
        return tryPutInternal(key, value, timeout, timeunit);
    }

    public V putIfAbsent(final K k, final V v) {
        return putIfAbsent(k, v, -1, null);
    }

    public V putIfAbsent(final K k, final V v, final long ttl, final TimeUnit timeunit) {
        final Data key = nodeEngine.toData(k);
        final Data value = nodeEngine.toData(v);
        final Data result = putIfAbsentInternal(key, value, ttl, timeunit);
        return toObject(result);
    }

    public void putTransient(final K k, final V v, final long ttl, final TimeUnit timeunit) {
        final Data key = nodeEngine.toData(k);
        final Data value = nodeEngine.toData(v);
        putTransientInternal(key, value, ttl, timeunit);
    }

    public boolean replace(final K k, final V o, final V v) {
        final Data key = nodeEngine.toData(k);
        final Data oldValue = nodeEngine.toData(o);
        final Data value = nodeEngine.toData(v);
        return replaceInternal(key,oldValue,value);
    }

    public V replace(final K k, final V v) {
        final Data key = nodeEngine.toData(k);
        final Data value = nodeEngine.toData(v);
        return toObject(replaceInternal(key, value));
    }

    public void set(final K k, final V v, final long ttl, final TimeUnit timeunit) {
        final Data key = nodeEngine.toData(k);
        final Data value = nodeEngine.toData(v);
        setInternal(key, value, ttl, timeunit);
    }

    public V remove(Object k) {
        final Data key = nodeEngine.toData(k);
        final Data result = removeInternal(key);
        return toObject(result);
    }

    public boolean remove(final Object k, final Object v) {
        final Data key = nodeEngine.toData(k);
        final Data value = nodeEngine.toData(v);
        return removeInternal(key, value);
    }

    public boolean containsKey(Object k) {
        Data key = nodeEngine.toData(k);
        return containsKeyInternal(key);
    }

    public boolean containsValue(final Object v) {
        Data value = nodeEngine.toData(v);
        return containsValueInternal(value);
    }

    public void lock(final K key) {
        Data k = nodeEngine.toData(key);
        lockInternal(k);
    }

    public void unlock(final K key) {
        Data k = nodeEngine.toData(key);
        unlockInternal(k);
    }

    public Object tryRemove(final K key, final long timeout, final TimeUnit timeunit) throws TimeoutException {
        Data k = nodeEngine.toData(key);
        return toObject(tryRemoveInternal(k, timeout, timeunit));
    }

    public Future<V> getAsync(final K k) {
        Data key = nodeEngine.toData(k);
        return null;
    }

    public Future<V> putAsync(final K key, final V value) {
        return null;
    }

    public Future<V> removeAsync(final K key) {
        return null;
    }

    public Map<K, V> getAll(final Set<K> keys) {
        return null;
    }

    public void putAll(final Map<? extends K, ? extends V> m) {
    }

    public boolean isLocked(final K key) {
        return false;
    }

    public boolean tryLock(final K key) {
        return false;
    }

    public boolean tryLock(final K key, final long time, final TimeUnit timeunit) {
        return false;
    }

    public void forceUnlock(final K key) {

    }

    public boolean lockMap(final long time, final TimeUnit timeunit) {
        return false;
    }

    public void unlockMap() {

    }

    public void addLocalEntryListener(final EntryListener<K, V> listener) {

    }

    public void addEntryListener(final EntryListener<K, V> listener, final boolean includeValue) {

    }

    public void removeEntryListener(final EntryListener<K, V> listener) {

    }

    public void addEntryListener(final EntryListener<K, V> listener, final K key, final boolean includeValue) {

    }

    public void removeEntryListener(final EntryListener<K, V> listener, final K key) {

    }

    public MapEntry<K, V> getMapEntry(final K key) {
        return null;
    }

    public boolean evict(final Object key) {
        return false;
    }

    public void clear() {

    }

    public void flush() {

    }

    public Set<K> keySet() {
        return null;
    }

    public Collection<V> values() {
        return null;
    }

    public Set<Entry<K, V>> entrySet() {
        return null;
    }

    public Set<K> keySet(final Predicate predicate) {
        return null;
    }

    public Set<Entry<K, V>> entrySet(final Predicate predicate) {
        return null;
    }

    public Collection<V> values(final Predicate predicate) {
        return null;
    }

    public Set<K> localKeySet() {
        return null;
    }

    public Set<K> localKeySet(final Predicate predicate) {
        return null;
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public InstanceType getInstanceType() {
        return InstanceType.MAP;
    }

    public void destroy() {
        super.destroy();
    }

    protected Object invoke(Operation operation, int partitionId) throws Throwable {
        Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId).build();
        Future f = invocation.invoke();
        Object response = f.get();
        Object returnObj;
        if (response instanceof Response) {
            Response r = (Response) response;
            returnObj = r.getResult();
        } else {
            returnObj = nodeEngine.toObject(response);
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
