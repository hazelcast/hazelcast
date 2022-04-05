/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.map.MapStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MapStoreWithCounter<K, V> implements MapStore<K, V> {

    protected final Map<K, V> store = new ConcurrentHashMap<K, V>();

    protected AtomicInteger countStore = new AtomicInteger(0);
    protected AtomicInteger countDelete = new AtomicInteger(0);
    protected AtomicInteger countLoad = new AtomicInteger(0);
    protected AtomicInteger batchCounter = new AtomicInteger(0);
    protected Map<Integer, Integer> batchOpCountMap = new ConcurrentHashMap<Integer, Integer>();

    public MapStoreWithCounter() {
    }

    @Override
    public void store(K key, V value) {
        countStore.incrementAndGet();
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<K, V> map) {
        batchOpCountMap.put(batchCounter.incrementAndGet(), map.size());

        countStore.addAndGet(map.size());
        for (Map.Entry<K, V> kvp : map.entrySet()) {
            store.put(kvp.getKey(), kvp.getValue());
        }
    }

    @Override
    public void delete(K key) {
        countDelete.incrementAndGet();
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<K> keys) {
        countDelete.addAndGet(keys.size());
        for (K key : keys) {
            store.remove(key);
        }
    }

    @Override
    public V load(K key) {
        countLoad.incrementAndGet();
        return store.get(key);
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        Map<K, V> result = new HashMap<K, V>();
        for (K key : keys) {
            final V v = store.get(key);
            if (v != null) {
                result.put(key, v);
            }
        }
        return result;
    }

    @Override
    public Set<K> loadAllKeys() {
        return store.keySet();
    }

    public int getStoreOpCount() {
        return countStore.intValue();
    }

    public int getDeleteOpCount() {
        return countDelete.intValue();
    }

    public int getLoadCount() {
        return countLoad.get();
    }

    public Map<Integer, Integer> getBatchOpCountMap() {
        return batchOpCountMap;
    }

    public int size() {
        return store.size();
    }

    public int findNumberOfBatchsEqualWriteBatchSize(int writeBatchSize) {
        int count = 0;
        final Map<Integer, Integer> batchOpCountMap = getBatchOpCountMap();
        final Collection<Integer> values = batchOpCountMap.values();
        for (Integer value : values) {
            if (value == writeBatchSize) {
                count++;
            }
        }
        return count;
    }
}
