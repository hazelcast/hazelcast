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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class EventBasedMapStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V> {

    public enum STORE_EVENTS {
        STORE, STORE_ALL, DELETE, DELETE_ALL, LOAD, LOAD_ALL, LOAD_ALL_KEYS
    }

    public final Map<K, V> store = new ConcurrentHashMap<K, V>();

    public final BlockingQueue<STORE_EVENTS> events = new LinkedBlockingQueue<STORE_EVENTS>();
    public final AtomicInteger storeCount = new AtomicInteger();
    public final AtomicInteger storeAllCount = new AtomicInteger();
    public final AtomicInteger loadCount = new AtomicInteger();
    public final AtomicInteger callCount = new AtomicInteger();
    public final AtomicInteger initCount = new AtomicInteger();
    public HazelcastInstance hazelcastInstance;
    public Properties properties;
    public String mapName;
    public boolean loadAllKeys = true;
    public CountDownLatch storeLatch;
    public CountDownLatch deleteLatch;
    public CountDownLatch loadAllLatch;

    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        this.hazelcastInstance = hazelcastInstance;
        this.properties = properties;
        this.mapName = mapName;
        initCount.incrementAndGet();
    }

    public BlockingQueue getEvents() {
        return events;
    }

    public void destroy() {
    }

    public int getEventCount() {
        return events.size();
    }

    public int getInitCount() {
        return initCount.get();
    }

    public boolean isLoadAllKeys() {
        return loadAllKeys;
    }

    public void setLoadAllKeys(boolean loadAllKeys) {
        this.loadAllKeys = loadAllKeys;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public String getMapName() {
        return mapName;
    }

    public Properties getProperties() {
        return properties;
    }

    public Map<K, V> getStore() {
        return store;
    }

    public EventBasedMapStore<K, V> insert(K key, V value) {
        store.put(key, value);
        return this;
    }

    public void store(K key, V value) {
        store.put(key, value);
        callCount.incrementAndGet();
        storeCount.incrementAndGet();
        if (storeLatch != null) {
            storeLatch.countDown();
        }
        events.offer(STORE_EVENTS.STORE);
    }

    public V load(K key) {
        callCount.incrementAndGet();
        loadCount.incrementAndGet();
        events.offer(STORE_EVENTS.LOAD);
        return store.get(key);
    }

    public void storeAll(Map<K, V> map) {
        store.putAll(map);
        callCount.incrementAndGet();
        storeAllCount.incrementAndGet();
        final int size = map.size();
        if (storeLatch != null) {
            for (int i = 0; i < size; i++) {
                storeLatch.countDown();
            }
        }
        events.offer(STORE_EVENTS.STORE_ALL);
    }

    public void delete(K key) {
        store.remove(key);
        callCount.incrementAndGet();
        if (deleteLatch != null) {
            deleteLatch.countDown();
        }
        events.offer(STORE_EVENTS.DELETE);
    }

    public Set<K> loadAllKeys() {
        if (loadAllLatch != null) {
            loadAllLatch.countDown();
        }
        callCount.incrementAndGet();
        events.offer(STORE_EVENTS.LOAD_ALL_KEYS);
        if (!loadAllKeys) {
            return null;
        }
        return store.keySet();
    }

    public Map<K, V> loadAll(Collection<K> keys) {
        Map<K, V> map = new HashMap<K, V>(keys.size());
        for (K key : keys) {
            V value = store.get(key);
            if (value != null) {
                map.put(key, value);
            }
        }
        callCount.incrementAndGet();
        events.offer(STORE_EVENTS.LOAD_ALL);
        return map;
    }

    public void deleteAll(Collection<K> keys) {
        for (K key : keys) {
            store.remove(key);
        }
        callCount.incrementAndGet();

        if (deleteLatch != null) {
            for (int i = 0; i < keys.size(); i++) {
                deleteLatch.countDown();
            }
        }
        events.offer(STORE_EVENTS.DELETE_ALL);
    }
}
