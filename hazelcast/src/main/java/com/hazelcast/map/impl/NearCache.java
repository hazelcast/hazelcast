/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NearCache.
 */
public class NearCache {
    /**
     * Used when caching nonexistent values.
     */
    public static final Object NULL_OBJECT = new Object();
    public static final String NEAR_CACHE_EXECUTOR_NAME = "hz:near-cache";
    private static final int HUNDRED_PERCENT = 100;
    private static final int EVICTION_PERCENTAGE = 20;
    private static final int CLEANUP_INTERVAL = 5000;
    private final int maxSize;
    private volatile long lastCleanup;
    private final long maxIdleMillis;
    private final long timeToLiveMillis;
    private final EvictionPolicy evictionPolicy;
    private final InMemoryFormat inMemoryFormat;
    private final NodeEngine nodeEngine;
    private final AtomicBoolean canCleanUp;
    private final AtomicBoolean canEvict;
    private final ConcurrentMap<Data, NearCacheRecord> cache;
    private final NearCacheStatsImpl nearCacheStats;
    private final SerializationService serializationService;
    private final Comparator<NearCacheRecord> selectedComparator;

    private SizeEstimator nearCacheSizeEstimator;

    /**
     * @param mapName    name of map which owns near cache.
     * @param nodeEngine node engine.
     */
    public NearCache(String mapName, NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        Config config = nodeEngine.getConfig();
        NearCacheConfig nearCacheConfig = config.findMapConfig(mapName).getNearCacheConfig();
        maxSize = nearCacheConfig.getMaxSize() <= 0 ? Integer.MAX_VALUE : nearCacheConfig.getMaxSize();
        maxIdleMillis = TimeUnit.SECONDS.toMillis(nearCacheConfig.getMaxIdleSeconds());
        inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        timeToLiveMillis = TimeUnit.SECONDS.toMillis(nearCacheConfig.getTimeToLiveSeconds());
        evictionPolicy = EvictionPolicy.valueOf(nearCacheConfig.getEvictionPolicy());
        selectedComparator = NearCacheRecord.getComparator(evictionPolicy);
        cache = new ConcurrentHashMap<Data, NearCacheRecord>();
        canCleanUp = new AtomicBoolean(true);
        canEvict = new AtomicBoolean(true);
        nearCacheStats = new NearCacheStatsImpl();
        lastCleanup = Clock.currentTimeMillis();
        serializationService = nodeEngine.getSerializationService();
    }

    // this operation returns the given value in near-cache memory format (data or object)
    public Object put(Data key, Data data) {
        fireTtlCleanup();
        if (evictionPolicy == EvictionPolicy.NONE && cache.size() >= maxSize) {
            // no more space in near-cache -> return given value in near-cache format
            if (data == null) {
                return null;
            } else {
                return inMemoryFormat.equals(InMemoryFormat.OBJECT) ? serializationService.toObject(data) : data;
            }
        }
        if (evictionPolicy != EvictionPolicy.NONE && cache.size() >= maxSize) {
            fireEvictCache();
        }
        final Object value;
        if (data == null) {
            value = NULL_OBJECT;
        } else {
            value = inMemoryFormat.equals(InMemoryFormat.OBJECT) ? serializationService.toObject(data) : data;
        }
        final NearCacheRecord record = new NearCacheRecord(key, value);
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
        for (NearCacheRecord record : cache.values()) {
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
                final ExecutionService executionService = nodeEngine.getExecutionService();
                executionService.execute(NEAR_CACHE_EXECUTOR_NAME, new Runnable() {
                    public void run() {
                        try {
                            TreeSet<NearCacheRecord> records = new TreeSet<NearCacheRecord>(selectedComparator);
                            records.addAll(cache.values());
                            int evictSize = cache.size() * EVICTION_PERCENTAGE / HUNDRED_PERCENT;
                            int i = 0;
                            for (NearCacheRecord record : records) {
                                cache.remove(record.getKey());
                                updateSizeEstimator(-calculateCost(record));
                                if (++i > evictSize) {
                                    break;
                                }
                            }
                        } finally {
                            canEvict.set(true);
                        }

                        if (cache.size() >= maxSize && canEvict.compareAndSet(true, false)) {
                            try {
                                executionService.execute(NEAR_CACHE_EXECUTOR_NAME, this);
                            } catch (RejectedExecutionException e) {
                                canEvict.set(true);
                            }
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
                nodeEngine.getExecutionService().execute(NEAR_CACHE_EXECUTOR_NAME, new Runnable() {
                    public void run() {
                        try {
                            lastCleanup = Clock.currentTimeMillis();
                            for (Map.Entry<Data, NearCacheRecord> entry : cache.entrySet()) {
                                if (entry.getValue().isExpired(maxIdleMillis, timeToLiveMillis)) {
                                    final Data key = entry.getKey();
                                    final NearCacheRecord record = cache.remove(key);
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
        NearCacheRecord record = cache.get(key);
        if (record != null) {
            if (record.isExpired(maxIdleMillis, timeToLiveMillis)) {
                cache.remove(key);
                updateSizeEstimator(-calculateCost(record));
                nearCacheStats.incrementMisses();
                return null;
            }
            nearCacheStats.incrementHits();
            record.access();
            return record.getValue();
        } else {
            nearCacheStats.incrementMisses();
            return null;
        }
    }

    public void invalidate(Data key) {
        final NearCacheRecord record = cache.remove(key);
        // if a mapping exists for the key.
        if (record != null) {
            updateSizeEstimator(-calculateCost(record));
        }
    }

    public void invalidate(Collection<Data> keys) {
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

    public Map<Data, NearCacheRecord> getReadonlyMap() {
        return Collections.unmodifiableMap(cache);
    }

    private void resetSizeEstimator() {
        getNearCacheSizeEstimator().reset();
    }

    private void updateSizeEstimator(long size) {
        getNearCacheSizeEstimator().add(size);
    }

    private long calculateCost(NearCacheRecord record) {
        return getNearCacheSizeEstimator().getCost(record);
    }

    public SizeEstimator getNearCacheSizeEstimator() {
        return nearCacheSizeEstimator;
    }

    public void setNearCacheSizeEstimator(SizeEstimator nearCacheSizeEstimator) {
        this.nearCacheSizeEstimator = nearCacheSizeEstimator;
    }
}
