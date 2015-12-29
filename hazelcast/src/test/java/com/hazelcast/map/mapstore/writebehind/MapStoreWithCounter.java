package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MapStoreWithCounter<K, V> implements MapStore<K, V> {

    protected final Map<K, V> store = new ConcurrentHashMap();

    protected AtomicInteger countStore = new AtomicInteger(0);
    protected AtomicInteger countDelete = new AtomicInteger(0);
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
        return store.get(key);
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        Map result = new HashMap();
        for (Object key : keys) {
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

    public Map<Integer, Integer> getBatchOpCountMap() {
        return batchOpCountMap;
    }

    public int size() {
        return store.size();
    }


    public int findNumberOfBatchsEqualWriteBatchSize(int writeBatchsize) {
        int count = 0;
        final Map<Integer, Integer> batchOpCountMap = getBatchOpCountMap();
        final Collection<Integer> values = batchOpCountMap.values();
        for (Integer value : values) {
            if (value == writeBatchsize) {
                count++;
            }
        }
        return count;
    }
}
