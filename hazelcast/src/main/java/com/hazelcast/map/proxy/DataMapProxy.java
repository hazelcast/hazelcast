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
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.IterationType;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class DataMapProxy extends MapProxySupport implements MapProxy<Data, Data> {

    public DataMapProxy(final String name, final MapService mapService, NodeEngine nodeEngine) {
        super(name, mapService, nodeEngine);
    }

    public Data get(Object k) {
        Data key = getService().toData(k);
        return getService().toData(getInternal(key));
    }

    public Future<Data> getAsync(final Data key) {
        return getAsyncInternal(key);
    }

    public Data put(final Data k, final Data v) {
        return put(k, v, -1, null);
    }

    public Data put(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        return putInternal(key, value, ttl, timeunit);
    }

    public boolean tryPut(final Data key, final Data value, final long timeout, final TimeUnit timeunit) {
        return tryPutInternal(key, value, timeout, timeunit);
    }

    public Data putIfAbsent(final Data k, final Data v) {
        return putIfAbsent(k, v, -1, null);
    }

    public Data putIfAbsent(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        return putIfAbsentInternal(key, value, ttl, timeunit);
    }

    public void putTransient(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        putTransientInternal(key, value, ttl, timeunit);
    }

    public Future<Data> putAsync(final Data key, final Data value) {
        return putAsyncInternal(key, value);
    }

    public boolean replace(final Data key, final Data oldValue, final Data newValue) {
        return replaceInternal(key, oldValue, newValue);
    }

    public Data replace(final Data key, final Data value) {
        return replaceInternal(key, value);
    }

    public void set(Data key, Data value) {
        setInternal(key, value, -1, null);
    }

    public void set(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        setInternal(key, value, ttl, timeunit);
    }

    public Data remove(Object k) {
        Data key = getService().toData(k);
        return removeInternal(key);
    }

    public boolean remove(final Object k, final Object v) {
        Data key = getService().toData(k);
        Data value = getService().toData(v);
        return removeInternal(key, value);
    }

    public void delete(Object k) {
        final NodeEngine nodeEngine = getNodeEngine();
        final Data key = getService().toData(k);
        deleteInternal(key);
    }

    public boolean tryRemove(final Data key, final long timeout, final TimeUnit timeunit) {
        return tryRemoveInternal(key, timeout, timeunit);
    }

    public Future<Data> removeAsync(final Data key) {
        return removeAsyncInternal(key);
    }

    public boolean containsKey(Object k) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data key = getService().toData(k);
        return containsKeyInternal(key);
    }

    public boolean containsValue(final Object value) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data v = getService().toData(value);
        return containsValueInternal(v);
    }

    public Map<Data, Data> getAll(final Set<Data> keys) {
        return getAllDataInternal(keys);
    }

    public void putAll(final Map<? extends Data, ? extends Data> m) {
        putAllDataInternal(m);
    }

    public void clear() {
        clearInternal();
    }

    public void lock(final Data key) {
        lockSupport.lock(getNodeEngine(), key);
    }

    public void lock(final Data key, final long leaseTime, final TimeUnit timeUnit) {
        lockSupport.lock(getNodeEngine(), key, timeUnit.toMillis(leaseTime));
    }

    public boolean isLocked(final Data key) {
        return lockSupport.isLocked(getNodeEngine(), key);
    }

    public boolean tryLock(final Data key) {
        return lockSupport.tryLock(getNodeEngine(), key);
    }

    public boolean tryLock(final Data key, final long time, final TimeUnit timeunit) throws InterruptedException {
        return lockSupport.tryLock(getNodeEngine(), key, time, timeunit);
    }

    public void unlock(final Data key) {
        lockSupport.unlock(getNodeEngine(), key);
    }

    public void forceUnlock(final Data key) {
        lockSupport.forceUnlock(getNodeEngine(), key);
    }

    public Set<Data> keySet() {
        return keySetInternal();
    }

    public Collection<Data> values() {
        return valuesInternal();
    }

    public Set<Entry<Data, Data>> entrySet() {
        return entrySetInternal();
    }

    public String addInterceptor(MapInterceptor interceptor) {
        return addMapInterceptorInternal(interceptor);
    }

    public void removeInterceptor(String id) {
        removeMapInterceptorInternal(id);
    }

    public String addEntryListener(final EntryListener<Data, Data> listener, final boolean includeValue) {
        return addEntryListenerInternal(listener, null, includeValue);
    }

    public String addEntryListener(EntryListener listener, Predicate predicate, Data key, boolean includeValue) {
        return addEntryListenerInternal(listener, predicate, key, includeValue);
    }

    public boolean removeEntryListener(final String id) {
        return removeEntryListenerInternal(id);
    }

    public String addEntryListener(final EntryListener<Data, Data> listener, final Data key, final boolean includeValue) {
        return addEntryListenerInternal(listener, key, includeValue);
    }

    @Override
    public EntryView<Data,Data> getEntryView(Data key) {
        return getEntryViewInternal(getService().toData(key));
    }

    public boolean evict(final Data key) {
        return evictInternal(key);
    }

    public Set<Data> keySet(final Predicate predicate) {
        return query(predicate, IterationType.KEY, true);
    }

    public Set<Entry<Data, Data>> entrySet(final Predicate predicate) {
        return query(predicate, IterationType.ENTRY, true);
    }

    public Collection<Data> values(final Predicate predicate) {
        return query(predicate, IterationType.VALUE, true);
    }

    public Set<Data> localKeySet() {
        return localKeySetInternal();
    }

    public Set<Data> localKeySet(final Predicate predicate) {
        return query(predicate, IterationType.KEY, true);
    }

    public Data executeOnKey(Data key, EntryProcessor entryProcessor) {
        return executeOnKeyInternal(key, entryProcessor);
    }
}
