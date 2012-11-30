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

package com.hazelcast.map.proxy;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEntry;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.NodeService;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DataMapProxy extends MapProxySupport implements MapProxy<Data, Data> {

    public DataMapProxy(final String name, final MapService mapService, NodeService nodeService) {
        super(name, mapService, nodeService);
    }

    public Data get(Object k) {
        Data key = nodeService.toData(k);
        return getInternal(key);
    }

    public Future<Data> getAsync(final Data key) {
        return null;
    }

    public Data put(final Data k, final Data v) {
        return put(k, v, -1, null);
    }

    public Data put(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
       return putInternal(key, value, ttl, timeunit);
    }

    public boolean tryPut(final Data key, final Data value, final long timeout, final TimeUnit timeunit) {
        return false;
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
        return null;
    }

    public boolean replace(final Data key, final Data oldValue, final Data newValue) {
        return replaceInternal(key, oldValue, newValue);
    }

    public Data replace(final Data key, final Data value) {
        return replaceInternal(key, value);
    }

    public void set(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        setInternal(key, value, ttl, timeunit);
    }

    public Data remove(Object k) {
        Data key = nodeService.toData(k);
        return removeInternal(key);
    }

    public boolean remove(final Object k, final Object v) {
        final Data key = nodeService.toData(k);
        final Data value = nodeService.toData(v);
        return removeInternal(key, value);
    }

    public Object tryRemove(final Data key, final long timeout, final TimeUnit timeunit) throws TimeoutException {
        return null;
    }

    public Future<Data> removeAsync(final Data key) {
        return null;
    }

    public boolean containsKey(Object k) {
        Data key = nodeService.toData(k);
        return containsKeyInternal(key);
    }

    public boolean containsValue(final Object value) {
        Data v = nodeService.toData(value);
        return containsValueInternal(v);
    }

    public Map<Data, Data> getAll(final Set<Data> keys) {
        return null;
    }

    public void putAll(final Map<? extends Data, ? extends Data> m) {

    }

    public void lock(final Data key) {
        lockInternal(key);
    }

    public boolean isLocked(final Data key) {
        return isLockedInternal(key);
    }

    public boolean tryLock(final Data key) {
        return tryLock(key, 0, TimeUnit.MILLISECONDS);
    }

    public boolean tryLock(final Data key, final long time, final TimeUnit timeunit) {
        return tryLockInternal(key, time, timeunit);
    }

    public void unlock(final Data key) {
        unlockInternal(key);
    }

    public void forceUnlock(final Data key) {
        forceUnlockInternal(key);
    }

    public void addLocalEntryListener(final EntryListener<Data, Data> listener) {

    }

    public void addEntryListener(final EntryListener<Data, Data> listener, final boolean includeValue) {

    }

    public void removeEntryListener(final EntryListener<Data, Data> listener) {

    }

    public void addEntryListener(final EntryListener<Data, Data> listener, final Data key, final boolean includeValue) {

    }

    public void removeEntryListener(final EntryListener<Data, Data> listener, final Data key) {

    }

    public MapEntry<Data, Data> getMapEntry(final Data key) {
        return null;
    }

    public boolean evict(final Object key) {
        return false;
    }

    public Set<Data> keySet() {
        return null;
    }

    public Collection<Data> values() {
        return null;
    }

    public Set<Entry<Data, Data>> entrySet() {
        return null;
    }

    public Set<Data> keySet(final Predicate predicate) {
        return null;
    }

    public Set<Entry<Data, Data>> entrySet(final Predicate predicate) {
        return null;
    }

    public Collection<Data> values(final Predicate predicate) {
        return null;
    }

    public Set<Data> localKeySet() {
        return null;
    }

    public Set<Data> localKeySet(final Predicate predicate) {
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

    }
}
