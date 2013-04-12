/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.proxy.nearcache;

import com.hazelcast.client.proxy.MapClientProxy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.util.Clock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class NearCache<K, V> {

    final int evictionPercentage = 20;
    final int cleanupInterval = 5000;
    volatile long lastCleanup;
    final MapConfig.EvictionPolicy evictionPolicy;
    final int maxSize;
    final int timeToLiveMillis;
    final int maxIdleMillis;
    final boolean invalidateOnChange;
    final ConcurrentMap<Object, CacheRecord> cache = new ConcurrentHashMap<Object, CacheRecord>();
    final CacheRecord NULLOBJECT = new CacheRecord(null, null, null, -1, -1);
    final AtomicBoolean canCleanUp = new AtomicBoolean(true);
    final AtomicBoolean canEvict = new AtomicBoolean(true);
    final ExecutorService ex = Executors.newFixedThreadPool(2);

    public NearCache(MapClientProxy<K, V> proxy, NearCacheConfig nc) {
        this.timeToLiveMillis = nc.getTimeToLiveSeconds() * 1000;
        this.maxSize = nc.getMaxSize();
        this.evictionPolicy = MapConfig.EvictionPolicy.valueOf(nc.getEvictionPolicy());
        this.maxIdleMillis = nc.getMaxIdleSeconds() * 1000;
        this.invalidateOnChange = nc.isInvalidateOnChange();
        if (invalidateOnChange) {
            proxy.addEntryListener(new EntryListener<K, V>() {
                @Override
                public void entryAdded(EntryEvent<K, V> kvEntryEvent) {
                    invalidate(kvEntryEvent.getKey());
                }

                @Override
                public void entryRemoved(EntryEvent<K, V> kvEntryEvent) {
                    invalidate(kvEntryEvent.getKey());
                }

                @Override
                public void entryUpdated(EntryEvent<K, V> kvEntryEvent) {
                    invalidate(kvEntryEvent.getKey());
                }

                @Override
                public void entryEvicted(EntryEvent<K, V> kvEntryEvent) {
                    invalidate(kvEntryEvent.getKey());
                }
            }, false);
        }
    }

    public V get(Object key) {
        fireCleanup();
        CacheRecord record = cache.get(key);
        if (record != null) {
            record.access();
            if (record.expired()) {
                cache.remove(key);
                return null;
            }
            return (V) record.value;
        } else {
            return null;
        }
    }

    public void put(Object key, V value) {
        fireCleanup();
        if (evictionPolicy == MapConfig.EvictionPolicy.NONE && cache.size() >= maxSize) {
            return;
        }
        if (evictionPolicy != MapConfig.EvictionPolicy.NONE && cache.size() >= maxSize) {
            fireEvictCache();
        }
        if (value == null) cache.put(key, NULLOBJECT);
        else cache.put(key, new CacheRecord(key, value, evictionPolicy, maxIdleMillis, timeToLiveMillis));
    }

    public void invalidate(K key) {
        cache.remove(key);
    }

    void clear() {
        cache.clear();
    }

    private void fireEvictCache() {
        if (canEvict.compareAndSet(true, false)) {
            ex.execute(new Runnable() {
                public void run() {
                    List<CacheRecord> values = new ArrayList(cache.values());
                    Collections.sort(values);
                    int evictSize = Math.min(values.size(), cache.size() * evictionPercentage / 100);
                    for (int i = 0; i < evictSize; i++) {
                        cache.remove(values.get(i).key);
                    }
                    canEvict.set(true);
                }
            });
        }
    }

    private void fireCleanup() {
        if (Clock.currentTimeMillis() < (lastCleanup + cleanupInterval))
            return;
        if (canCleanUp.compareAndSet(true, false)) {
            ex.execute(new Runnable() {
                public void run() {
                    lastCleanup = Clock.currentTimeMillis();
                    Iterator<Map.Entry<Object, CacheRecord>> iterator = cache.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Object, CacheRecord> entry = iterator.next();
                        if (entry.getValue().expired()) {
                            cache.remove(entry.getKey());
                        }
                    }
                    canCleanUp.set(true);
                }
            });
        }
    }
}
