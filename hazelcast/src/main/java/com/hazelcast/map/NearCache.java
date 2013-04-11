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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NearCache {

    final int evictionPercentage = 20;
    final int cleanupInterval = 5000;
    final int maxSize;
    volatile long lastCleanup;
    final long maxIdleMillis;
    final long timeToLiveMillis;
    final boolean invalidateOnChange;
    final EvictionPolicy evictionPolicy;
    final MapConfig.InMemoryFormat inMemoryFormat;
    final String mapName;
    final MapService mapService;
    final NodeEngine nodeEngine;
    final AtomicBoolean canCleanUp;
    final AtomicBoolean canEvict;
    final ConcurrentMap<Data, CacheRecord> cache;

    public NearCache(String mapName, MapService mapService) {
        this.mapName = mapName;
        this.mapService = mapService;
        this.nodeEngine = mapService.getNodeEngine();
        Config config = nodeEngine.getConfig();
        inMemoryFormat = config.getMapConfig(mapName).getInMemoryFormat();
        NearCacheConfig nearCacheConfig = config.getMapConfig(mapName).getNearCacheConfig();
        maxSize = nearCacheConfig.getMaxSize();
        maxIdleMillis = nearCacheConfig.getMaxIdleSeconds() * 1000;
        timeToLiveMillis = nearCacheConfig.getTimeToLiveSeconds() * 1000;
        invalidateOnChange = nearCacheConfig.isInvalidateOnChange();
        evictionPolicy = EvictionPolicy.valueOf(nearCacheConfig.getEvictionPolicy());
        cache = new ConcurrentHashMap<Data, CacheRecord>();
        canCleanUp = new AtomicBoolean(true);
        canEvict = new AtomicBoolean(true);
        lastCleanup = Clock.currentTimeMillis();
    }

    static enum EvictionPolicy {
        NONE, LRU, LFU
    }

    public void put(Data key, Data value) {
        fireCleanup();
        if (evictionPolicy == EvictionPolicy.NONE && cache.size() >= maxSize) {
            return;
        }
        if (evictionPolicy != EvictionPolicy.NONE && cache.size() >= maxSize) {
                fireEvictCache();
        }
        cache.put(key, new CacheRecord(key, value));
    }

    private void fireEvictCache() {
        if (canEvict.compareAndSet(true, false)) {
            nodeEngine.getExecutionService().execute("hz:near-cache", new Runnable() {
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
            nodeEngine.getExecutionService().execute("hz:near-cache", new Runnable() {
                public void run() {
                    lastCleanup = Clock.currentTimeMillis();
                    Iterator<Map.Entry<Data, CacheRecord>> iterator = cache.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Data, CacheRecord> entry = iterator.next();
                        if (entry.getValue().expired()) {
                            cache.remove(entry.getKey());
                        }
                    }
                    canCleanUp.set(true);
                }
            });
        }
    }

    public Data get(Data key) {
        fireCleanup();
        CacheRecord record = cache.get(key);
        if (record != null) {
            record.access();
            if (record.expired()) {
                cache.remove(key);
                return null;
            }
            return record.value;
        } else {
            return null;
        }
    }

    public void invalidate(Data key) {
        cache.remove(key);
    }

    void clear() {
        cache.clear();
    }

    class CacheRecord implements Comparable<CacheRecord> {
        final Data key;
        final Data value;
        volatile long lastAccessTime;
        final long creationTime;
        final AtomicInteger hit;

        CacheRecord(Data key, Data value) {
            this.key = key;
            this.value = value;
            long time = Clock.currentTimeMillis();
            this.lastAccessTime = time;
            this.creationTime = time;
            this.hit = new AtomicInteger(0);
        }

        void access() {
            hit.incrementAndGet();
            lastAccessTime = Clock.currentTimeMillis();
        }

        boolean expired() {
            long time = Clock.currentTimeMillis();
            return (maxIdleMillis > 0 && time > lastAccessTime + maxIdleMillis) || (timeToLiveMillis > 0 && time > creationTime + timeToLiveMillis);
        }

        public int compareTo(CacheRecord o) {
            if (evictionPolicy.equals("LRU"))
                return ((Long) this.lastAccessTime).compareTo((o.lastAccessTime));
            else if (evictionPolicy.equals("LFU"))
                return ((Integer) this.hit.get()).compareTo((o.hit.get()));

            return 0;
        }
    }
}
