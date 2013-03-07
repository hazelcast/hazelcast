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

package com.hazelcast.collection.multimap;

import com.hazelcast.collection.*;
import com.hazelcast.collection.operations.CollectionResponse;
import com.hazelcast.collection.operations.EntrySetResponse;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MultiMap;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @ali 1/2/13
 */
public class ObjectMultiMapProxy<K, V> extends MultiMapProxySupport implements CollectionProxy, MultiMap<K, V> {

    public ObjectMultiMapProxy(CollectionService service, NodeEngine nodeEngine, CollectionProxyId proxyId) {
        super(service, nodeEngine, nodeEngine.getConfig().getMultiMapConfig(proxyId.getName()), proxyId);
    }

    public boolean put(K key, V value) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        return putInternal(dataKey, dataValue, -1);
    }

    public Collection<V> get(K key) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        CollectionResponse result = getAllInternal(dataKey);
        return result.getObjectCollection(nodeEngine);
    }

    public boolean remove(Object key, Object value) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        return removeInternal(dataKey, dataValue);
    }

    public Collection<V> remove(Object key) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        CollectionResponse result = removeInternal(dataKey);
        return result.getObjectCollection(nodeEngine);
    }

    public Set<K> localKeySet() {
        Set<Data> dataKeySet = localKeySetInternal();
        return toObjectSet(dataKeySet);
    }

    public Set<K> keySet() {
        Set<Data> dataKeySet = keySetInternal();
        return toObjectSet(dataKeySet);
    }

    public Collection<V> values() {
        final NodeEngine nodeEngine = getNodeEngine();
        Map map = valuesInternal();
        Collection values = new LinkedList();
        for (Object obj : map.values()) {
            if (obj == null) {
                continue;
            }
            CollectionResponse response = nodeEngine.toObject(obj);
            values.addAll(response.getObjectCollection(nodeEngine));
        }
        return values;
    }

    public Set<Map.Entry<K, V>> entrySet() {
        final NodeEngine nodeEngine = getNodeEngine();
        Map map = entrySetInternal();
        Set<Map.Entry<K, V>> entrySet = new HashSet<Map.Entry<K, V>>();
        for (Object obj : map.values()) {
            if (obj == null) {
                continue;
            }
            EntrySetResponse response = nodeEngine.toObject(obj);
            Set<Map.Entry<K, V>> entries = response.getObjectEntrySet(nodeEngine);
            entrySet.addAll(entries);
        }
        return entrySet;
    }

    public boolean containsKey(K key) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return containsInternal(dataKey, null);
    }

    public boolean containsValue(Object value) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data valueKey = nodeEngine.toData(value);
        return containsInternal(null, valueKey);
    }

    public boolean containsEntry(K key, V value) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        Data valueKey = nodeEngine.toData(value);
        return containsInternal(dataKey, valueKey);
    }

    public int valueCount(K key) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return countInternal(dataKey);
    }

    public void addLocalEntryListener(EntryListener<K, V> listener) {
        getService().addListener(proxyId.getName(), listener, null, false, true);
    }

    public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        getService().addListener(proxyId.getName(), listener, null, includeValue, false);
    }

    public void removeEntryListener(EntryListener<K, V> listener) {
        getService().removeListener(proxyId.getName(), listener, null);
    }

    public void addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        getService().addListener(proxyId.getName(), listener, dataKey, includeValue, false);
    }

    public void removeEntryListener(EntryListener<K, V> listener, K key) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        getService().removeListener(proxyId.getName(), listener, dataKey);
    }

    public void lock(K key) {
        try {
            tryLock(key, -1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            ExceptionUtil.sneakyThrow(e);
        }
    }

    public boolean tryLock(K key) {
        try {
            return tryLock(key, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        return lockSupport.tryLock(nodeEngine, dataKey, time, timeunit);
    }

    public void unlock(K key) {
        final NodeEngine nodeEngine = getNodeEngine();
        Data dataKey = nodeEngine.toData(key);
        lockSupport.unlock(nodeEngine, dataKey);
    }

    public LocalMapStats getLocalMultiMapStats() {
        return getService().createStats(proxyId);
    }

    private Set<K> toObjectSet(Set<Data> dataSet) {
        final NodeEngine nodeEngine = getNodeEngine();
        Set<K> keySet = new HashSet<K>(dataSet.size());
        for (Data dataKey : dataSet) {
            keySet.add((K) nodeEngine.toObject(dataKey));
        }
        return keySet;
    }
}
