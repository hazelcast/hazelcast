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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * NearCache.
 */
public class NearCache {
    /**
     * Used when caching nonexistent values.
     */
    public static final Object NULL_OBJECT = new Object();
    private static final int HUNDRED_PERCENT = 100;
    private static final int EVICTION_PERCENTAGE = 20;
    private static final int CLEANUP_INTERVAL = 5000;
    private final int maxSize;
    private volatile long lastCleanup;
    private final long maxIdleMillis;
    private final long timeToLiveMillis;
    private final EvictionPolicy evictionPolicy;
    private final InMemoryFormat inMemoryFormat;
    private final MapService mapService;
    private final NodeEngine nodeEngine;
    private final AtomicBoolean canCleanUp;
    private final AtomicBoolean canEvict;
    private final ConcurrentMap<Data, CacheRecord> cache;
    private final MapContainer mapContainer;
    private final NearCacheStatsImpl nearCacheStats;

    /**
     * @param mapName
     * @param mapService
     */
    public NearCache(String mapName, MapService mapService) {
        this.mapService = mapService;
        this.nodeEngine = mapService.getNodeEngine();
        this.mapContainer = mapService.getMapContainer(mapName);
        Config config = nodeEngine.getConfig();
        NearCacheConfig nearCacheConfig = config.findMapConfig(mapName).getNearCacheConfig();
        maxSize = nearCacheConfig.getMaxSize() <= 0 ? Integer.MAX_VALUE : nearCacheConfig.getMaxSize();
        maxIdleMillis = TimeUnit.SECONDS.toMillis(nearCacheConfig.getMaxIdleSeconds());
        inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        timeToLiveMillis = TimeUnit.SECONDS.toMillis(nearCacheConfig.getTimeToLiveSeconds());
        evictionPolicy = EvictionPolicy.valueOf(nearCacheConfig.getEvictionPolicy());
        cache = new ConcurrentHashMap<Data, CacheRecord>();
        canCleanUp = new AtomicBoolean(true);
        canEvict = new AtomicBoolean(true);
        nearCacheStats = new NearCacheStatsImpl();
        lastCleanup = Clock.currentTimeMillis();
    }

    /**
     * EvictionPolicy.
     */
    static enum EvictionPolicy {
        NONE, LRU, LFU
    }

    // this operation returns the given value in near-cache memory format (data or object)
    public Object put(Data key, Data data) {
        fireTtlCleanup();
        if (evictionPolicy == EvictionPolicy.NONE && cache.size() >= maxSize) {
            // no more space in near-cache -> return given value in near-cache format
            if (data == null) {
                return null;
            } else {
                return inMemoryFormat.equals(InMemoryFormat.OBJECT) ? mapService.toObject(data) : data;
            }
        }
        if (evictionPolicy != EvictionPolicy.NONE && cache.size() >= maxSize) {
            fireEvictCache();
        }
        final Object value;
        if (data == null) {
            value = NULL_OBJECT;
        } else {
            value = inMemoryFormat.equals(InMemoryFormat.OBJECT) ? mapService.toObject(data) : data;
        }
        final CacheRecord record = new CacheRecord(key, value);
        cache.put(key, record);
        updateSizeEstimator(calculateCost(record));
        if (NULL_OBJECT.equals(value)) {
            return null;
        } else {
            return value;
        }
    }

    public NearCacheStatsImpl getNearCacheStats() {
        return createNearCacheStats();
    }

    private NearCacheStatsImpl createNearCacheStats() {
        long ownedEntryCount = 0;
        long ownedEntryMemoryCost = 0;
        for (CacheRecord record : cache.values()) {
            ownedEntryCount++;
            ownedEntryMemoryCost += record.getCost();
        }
        nearCacheStats.setOwnedEntryCount(ownedEntryCount);
        nearCacheStats.setOwnedEntryMemoryCost(ownedEntryMemoryCost);
        return nearCacheStats;
    }

    private void fireEvictCache() {
        if (canEvict.compareAndSet(true, false)) {
            try {
                nodeEngine.getExecutionService().execute("hz:near-cache", new Runnable() {
                    public void run() {
                        try {
                            TreeSet<CacheRecord> records = new TreeSet<CacheRecord>(cache.values());
                            int evictSize = cache.size() * EVICTION_PERCENTAGE / HUNDRED_PERCENT;
                            int i = 0;
                            for (CacheRecord record : records) {
                                cache.remove(record.key);
                                updateSizeEstimator(-calculateCost(record));
                                if (++i > evictSize) {
                                    break;
                                }
                            }
                        } finally {
                            canEvict.set(true);
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                canEvict.set(true);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
    }

    private void fireTtlCleanup() {
        if (Clock.currentTimeMillis() < (lastCleanup + CLEANUP_INTERVAL)) {
            return;
        }

        if (canCleanUp.compareAndSet(true, false)) {
            try {
                nodeEngine.getExecutionService().execute("hz:near-cache", new Runnable() {
                    public void run() {
                        try {
                            lastCleanup = Clock.currentTimeMillis();
                            for (Map.Entry<Data, CacheRecord> entry : cache.entrySet()) {
                                if (entry.getValue().expired()) {
                                    final Data key = entry.getKey();
                                    final CacheRecord record = cache.remove(key);
                                    //if a mapping exists.
                                    if (record != null) {
                                        updateSizeEstimator(-calculateCost(record));
                                    }
                                }
                            }
                        } finally {
                            canCleanUp.set(true);
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                canCleanUp.set(true);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
    }

    public Object get(Data key) {
        fireTtlCleanup();
        CacheRecord record = cache.get(key);
        if (record != null) {
            if (record.expired()) {
                cache.remove(key);
                updateSizeEstimator(-calculateCost(record));
                nearCacheStats.incrementMisses();
                return null;
            }
            record.access();
            return record.value;
        } else {
            nearCacheStats.incrementMisses();
            return null;
        }
    }

    public void invalidate(Data key) {
        final CacheRecord record = cache.remove(key);
        // if a mapping exists for the key.
        if (record != null) {
            updateSizeEstimator(-calculateCost(record));
        }
    }

    public void invalidate(Set<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        for (Data key : keys) {
            invalidate(key);
        }
    }

    public int size() {
        return cache.size();
    }

    public void clear() {
        cache.clear();
        resetSizeEstimator();
    }

    public Map<Data, CacheRecord> getReadonlyMap() {
        return Collections.unmodifiableMap(cache);
    }

    /**
     * CacheRecord.
     */
    public class CacheRecord implements Comparable<CacheRecord> {
        final Data key;
        final Object value;
        final long creationTime;
        final AtomicInteger hit;
        volatile long lastAccessTime;

        CacheRecord(Data key, Object value) {
            this.key = key;
            this.value = value;
            long time = Clock.currentTimeMillis();
            this.lastAccessTime = time;
            this.creationTime = time;
            this.hit = new AtomicInteger(0);
        }

        void access() {
            hit.incrementAndGet();
            nearCacheStats.incrementHits();
            lastAccessTime = Clock.currentTimeMillis();
        }

        boolean expired() {
            long time = Clock.currentTimeMillis();
            return (maxIdleMillis > 0 && time > lastAccessTime + maxIdleMillis)
                    || (timeToLiveMillis > 0 && time > creationTime + timeToLiveMillis);
        }

        public int compareTo(CacheRecord o) {
            if (EvictionPolicy.LRU.equals(evictionPolicy)) {
                return ((Long) this.lastAccessTime).compareTo((o.lastAccessTime));
            } else if (EvictionPolicy.LFU.equals(evictionPolicy)) {
                return ((Integer) this.hit.get()).compareTo((o.hit.get()));
            }
            return 0;
        }

        public boolean equals(Object o) {
            if (o != null && o instanceof CacheRecord) {
                return this.compareTo((CacheRecord) o) == 0;
            }
            return false;
        }

        // If you don't think instances of this class will ever be inserted into a HashMap/HashTable,
        // the recommended hashCode implementation to use is:
        public int hashCode() {
            assert false : "hashCode not designed";
            // any arbitrary constant will do.
            return 42;
        }

        public long getCost() {
            // todo find object size  if not a Data instance.
            if (!(value instanceof Data)) {
                return 0;
            }
            final int numberOfLongs = 2;
            final int numberOfIntegers = 3;
            // value is Data
            return key.getHeapCost()
                    + ((Data) value).getHeapCost()
                    + numberOfLongs * (Long.SIZE / Byte.SIZE)
                    // sizeof atomic integer
                    + (Integer.SIZE / Byte.SIZE)
                    // object references (key, value, hit)
                    + numberOfIntegers * (Integer.SIZE / Byte.SIZE);
        }

        public Data getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }
    }

    private void resetSizeEstimator() {
        mapContainer.getNearCacheSizeEstimator().reset();
    }

    private void updateSizeEstimator(long size) {
        mapContainer.getNearCacheSizeEstimator().add(size);
    }

    private long calculateCost(CacheRecord record) {
        return mapContainer.getNearCacheSizeEstimator().getCost(record);
    }
}
