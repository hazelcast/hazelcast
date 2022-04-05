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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.EntryStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertBetween;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestEntryStore<K, V> implements EntryStore<K, V> {

    private static final long NO_TIME = -1;

    private ConcurrentMap<K, Record> records = new ConcurrentHashMap<>();
    private AtomicInteger loadedEntryCount = new AtomicInteger();
    private AtomicInteger loadAllCallCount = new AtomicInteger();
    private AtomicInteger loadCallCount = new AtomicInteger();
    private AtomicInteger loadKeysCallCount = new AtomicInteger();
    private AtomicInteger storedEntryCount = new AtomicInteger();
    private AtomicInteger storeCallCount = new AtomicInteger();
    private AtomicInteger storeAllCallCount = new AtomicInteger();
    private AtomicInteger deleteCallCount = new AtomicInteger();

    @Override
    public MetadataAwareValue<V> load(K key) {
        loadCallCount.incrementAndGet();
        Record record = records.get(key);
        if (record == null) {
            return null;
        }
        loadedEntryCount.incrementAndGet();
        if (record.expirationTime == NO_TIME) {
            return new MetadataAwareValue<>(record.value);
        } else {
            return new MetadataAwareValue<>(record.value, record.expirationTime);
        }
    }

    @Override
    public Map<K, MetadataAwareValue<V>> loadAll(Collection<K> keys) {
        loadAllCallCount.incrementAndGet();
        loadedEntryCount.addAndGet(keys.size());
        Map<K, MetadataAwareValue<V>> map = new HashMap<>(keys.size());
        for (K key: keys) {
            Record record = records.get(key);
            map.put(key, new MetadataAwareValue<>(record.value, record.expirationTime));
        }
        return map;
    }

    @Override
    public Iterable<K> loadAllKeys() {
        loadKeysCallCount.incrementAndGet();
        return records.keySet();
    }

    @Override
    public void store(K key, MetadataAwareValue<V> value) {
        V internalValue = value.getValue();
        long expirationTime = value.getExpirationTime();
        if (expirationTime == MetadataAwareValue.NO_TIME_SET) {
            records.put(key, new Record(internalValue, NO_TIME));
        } else {
            records.put(key, new Record(internalValue, expirationTime));
        }
        storeCallCount.incrementAndGet();
        storedEntryCount.incrementAndGet();
    }

    @Override
    public void storeAll(Map<K, MetadataAwareValue<V>> map) {
        for (Map.Entry<K, MetadataAwareValue<V>> mapEntry: map.entrySet()) {
            K key = mapEntry.getKey();
            MetadataAwareValue<V> entry = mapEntry.getValue();
            V internalValue = entry.getValue();
            long expirationTime = entry.getExpirationTime();
            if (expirationTime == MetadataAwareValue.NO_TIME_SET) {
                records.put(key, new Record(internalValue, NO_TIME));
            } else {
                records.put(key, new Record(internalValue, expirationTime));
            }
        }
        storedEntryCount.addAndGet(map.size());
        storeAllCallCount.incrementAndGet();
    }

    @Override
    public void delete(K key) {
        records.remove(key);
        deleteCallCount.incrementAndGet();
    }

    @Override
    public void deleteAll(Collection<K> keys) {
        for (K key: keys) {
            records.remove(key);
        }
    }

    public void putExternally(K key, V value, long expirationTime) {
        this.records.put(key, new Record(value, expirationTime));
    }

    public void putExternally(K key, V value) {
        this.records.put(key, new Record(value, Long.MAX_VALUE));
    }

    public int getLoadCallCount() {
        return loadCallCount.get();
    }

    public int getLoadAllCallCount() {
        return loadAllCallCount.get();
    }

    public int getLoadAllKeysCallCount() {
        return loadKeysCallCount.get();
    }

    public int getLoadedEntryCount() {
        return loadedEntryCount.get();
    }

    public int getLoadKeysCallCount() {
        return loadKeysCallCount.get();
    }

    public int getStoredEntryCount() {
        return storedEntryCount.get();
    }

    public int getStoreCallCount() {
        return storeCallCount.get();
    }

    public int getStoreAllCallCount() {
        return storeAllCallCount.get();
    }

    public int getDeleteCallCount() {
        return deleteCallCount.get();
    }

    public void assertRecordStored(K key, V value, long expectedExpirationTime, long delta) {
        Record record = records.get(key);
        assertNotNull(record);
        assertEquals(value, record.value);
        assertBetween("expirationTime", record.expirationTime,
                expectedExpirationTime - delta, expectedExpirationTime + delta);
    }

    public void assertRecordNotStored(K key) {
        Record record = records.get(key);
        assertNull(record);
    }

    public void assertRecordStored(K key, V value) {
        Record record = records.get(key);
        assertNotNull(record);
        assertEquals(value, record.value);
        assertEquals(NO_TIME, record.expirationTime);
    }

    private class Record {
        public V value;
        public long expirationTime;

        private Record(V value, long expirationTime) {
            this.value = value;
            this.expirationTime = expirationTime;
        }
    }
}
