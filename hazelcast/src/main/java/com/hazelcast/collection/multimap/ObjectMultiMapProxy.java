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
import com.hazelcast.map.LockInfo;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @ali 1/2/13
 */
public class ObjectMultiMapProxy<K, V> extends MultiMapProxySupport implements CollectionProxy, MultiMap<K, V> {

    public ObjectMultiMapProxy(String name, CollectionService service, NodeEngine nodeEngine, CollectionProxyType proxyType) {
        super(name, service, nodeEngine, proxyType, nodeEngine.getConfig().getMultiMapConfig(name));
    }

    public boolean put(K key, V value) {
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        return putInternal(dataKey, dataValue, -1);
    }

    public Collection<V> get(K key) {
        Data dataKey = nodeEngine.toData(key);
        CollectionResponse result = getAllInternal(dataKey);
        return result.getObjectCollection(nodeEngine);
    }

    public boolean remove(Object key, Object value) {
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        return removeInternal(dataKey, dataValue);
    }

    public Collection<V> remove(Object key) {
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
        Data dataKey = nodeEngine.toData(key);
        return containsInternal(dataKey, null);
    }

    public boolean containsValue(Object value) {
        Data valueKey = nodeEngine.toData(value);
        return containsInternal(null, valueKey);
    }

    public boolean containsEntry(K key, V value) {
        Data dataKey = nodeEngine.toData(key);
        Data valueKey = nodeEngine.toData(value);
        return containsInternal(dataKey, valueKey);
    }


    public int valueCount(K key) {
        Data dataKey = nodeEngine.toData(key);
        return countInternal(dataKey);
    }

    public void addLocalEntryListener(EntryListener<K, V> listener) {
        service.addListener(name, listener, null, false, true);
    }

    public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        service.addListener(name, listener, null, includeValue, false);
    }

    public void removeEntryListener(EntryListener<K, V> listener) {
        service.removeListener(name, listener, null);
    }

    public void addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        Data dataKey = nodeEngine.toData(key);
        service.addListener(name, listener, dataKey, includeValue, false);
    }

    public void removeEntryListener(EntryListener<K, V> listener, K key) {
        Data dataKey = nodeEngine.toData(key);
        service.removeListener(name, listener, dataKey);
    }

    public void lock(K key) {
        tryLock(key, -1, TimeUnit.MILLISECONDS);
    }

    public boolean tryLock(K key) {
        return tryLock(key, 0, TimeUnit.MILLISECONDS);
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) {
        Data dataKey = nodeEngine.toData(key);
        return lockInternal(dataKey, timeunit.toMillis(time));
    }

    public void unlock(K key) {
        Data dataKey = nodeEngine.toData(key);
        unlockInternal(dataKey);
    }

    public LocalMapStats getLocalMultiMapStats() {
        int count = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < count; i++) {
            CollectionPartitionContainer partitionContainer = service.getPartitionContainer(i);
            Map<CollectionProxyId, CollectionContainer> multiMaps = partitionContainer.getContainerMap();
            if (multiMaps.size() > 0) {
                System.out.println("partitionId: " + i);
            }
            for (Map.Entry<CollectionProxyId, CollectionContainer> entry : multiMaps.entrySet()) {
                System.out.println("\tname: " + entry.getKey());
                CollectionContainer container = entry.getValue();
                Map<Data, Collection<CollectionRecord>> map = container.getCollections();
                for (Map.Entry<Data, Collection<CollectionRecord>> en : map.entrySet()) {
                    System.out.println("\t\tkey: " + nodeEngine.toObject(en.getKey()));
                    Collection<CollectionRecord> col = en.getValue();
                    for (CollectionRecord o : col) {
                        System.out.println("\t\t\tval: " + nodeEngine.toObject(o.getObject()) + ", id: " + o.getRecordId());
                    }
                }
                ConcurrentMap<Data, LockInfo> locks = container.getLocks();
                System.out.println("\t\t\t\t\t-------------");
                for (Data key : locks.keySet()) {
                    System.out.println("\t\t\t\t\t\tkey: " + nodeEngine.toObject(key));
                }
                System.out.println("\t\t\t\t\t-------------");
            }

        }
        return null;
    }

    private Set<K> toObjectSet(Set<Data> dataSet) {
        Set<K> keySet = new HashSet<K>(dataSet.size());
        for (Data dataKey : dataSet) {
            keySet.add((K) nodeEngine.toObject(dataKey));
        }
        return keySet;
    }
}
