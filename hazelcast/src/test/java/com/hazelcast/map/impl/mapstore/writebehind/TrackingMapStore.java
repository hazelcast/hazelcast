/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TrackingMapStore<K, V> implements MapStore<K, V> {

    private static final long WRITE_DELAY_MILLIS = 1;

    private final Map<K, V> store = new ConcurrentHashMap<>();
    private final Map<K, V> expectedWrites = new ConcurrentHashMap<>();

    private final AtomicInteger storeCount = new AtomicInteger();
    private final AtomicInteger duplicateStoreCount = new AtomicInteger();
    private final AtomicInteger deleteCount = new AtomicInteger();

    private volatile long lastWrite = System.currentTimeMillis();

    @Override
    public V load(K key) {
        return store.get(key);
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        Map<K, V> loadedEntries = new HashMap<>();
        for (K key : keys) {
            V value = load(key);
            if (value != null) {
                loadedEntries.put(key, value);
            }
        }
        return loadedEntries;
    }

    @Override
    public Iterable<K> loadAllKeys() {
        return store.keySet();
    }

    @Override
    public void store(K key, V value) {
        recordWrite(key, value);
        pauseAfterWrite();
        completeExpectedWrite(key, value);
        markWriteComplete();
    }

    @Override
    public void storeAll(Map<K, V> map) {
        map.forEach(this::recordWrite);
        pauseAfterWrite();
        map.forEach(this::completeExpectedWrite);
        markWriteComplete();
    }

    @Override
    public void delete(K key) {
        store.remove(key);
        deleteCount.incrementAndGet();
    }

    @Override
    public void deleteAll(Collection<K> keys) {
        keys.forEach(this::delete);
    }

    public void expectWrite(K key, V value) {
        expectedWrites.put(key, value);
    }

    public boolean allExpectedWritesComplete() {
        return expectedWrites.isEmpty();
    }

    public boolean lastWriteAtLeastMsAgo(long ms) {
        return System.currentTimeMillis() - lastWrite >= ms;
    }

    public void reset() {
        store.clear();
        expectedWrites.clear();
        storeCount.set(0);
        duplicateStoreCount.set(0);
        deleteCount.set(0);
        lastWrite = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "TrackingMapStore{"
                + "storeSize=" + store.size()
                + ", expectedWriteCount=" + expectedWrites.size()
                + ", lastWrite=" + lastWrite
                + ", storeCount=" + storeCount
                + ", duplicateStoreCount=" + duplicateStoreCount
                + ", deleteCount=" + deleteCount
                + '}';
    }

    private void recordWrite(K key, V value) {
        if (store.put(key, value) != null) {
            duplicateStoreCount.incrementAndGet();
        }
        storeCount.incrementAndGet();
    }

    private void completeExpectedWrite(K key, V value) {
        expectedWrites.remove(key, value);
    }

    private void markWriteComplete() {
        lastWrite = System.currentTimeMillis();
    }

    private void pauseAfterWrite() {
        try {
            Thread.sleep(WRITE_DELAY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
