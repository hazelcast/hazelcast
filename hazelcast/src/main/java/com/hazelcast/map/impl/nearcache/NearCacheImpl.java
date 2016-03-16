/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.impl.SizeEstimator;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.EvictionPolicy.NONE;

/**
 * NearCache.
 */
public class NearCacheImpl implements NearCache<Data, Object> {
    public static final String NEAR_CACHE_EXECUTOR_NAME = "hz:near-cache";
    private static final double EVICTION_FACTOR = 0.2;
    private static final int CLEANUP_INTERVAL = 5000;
    private final int maxSize;
    private final String mapName;
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
    private final boolean invalidateOnChange;

    private SizeEstimator nearCacheSizeEstimator;

    /**
     * @param mapName    name of map which owns near cache.
     * @param nodeEngine node engine.
     */
    public NearCacheImpl(String mapName, NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        Config config = nodeEngine.getConfig();
        NearCacheConfig nearCacheConfig = config.findMapConfig(mapName).getNearCacheConfig();
        this.maxSize = nearCacheConfig.getMaxSize() <= 0 ? Integer.MAX_VALUE : nearCacheConfig.getMaxSize();
        this.maxIdleMillis = TimeUnit.SECONDS.toMillis(nearCacheConfig.getMaxIdleSeconds());
        this.inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        this.timeToLiveMillis = TimeUnit.SECONDS.toMillis(nearCacheConfig.getTimeToLiveSeconds());
        this.evictionPolicy = EvictionPolicy.valueOf(nearCacheConfig.getEvictionPolicy());
        this.selectedComparator = NearCacheRecord.getComparator(evictionPolicy);
        this.cache = new ConcurrentHashMap<Data, NearCacheRecord>();
        this.canCleanUp = new AtomicBoolean(true);
        this.canEvict = new AtomicBoolean(true);
        this.nearCacheStats = new NearCacheStatsImpl();
        this.lastCleanup = Clock.currentTimeMillis();
        this.serializationService = nodeEngine.getSerializationService();
        this.invalidateOnChange = nearCacheConfig.isInvalidateOnChange();
        this.mapName = mapName;
    }

    // TODO this operation returns the given value in near-cache memory format (data or object)?
    @Override
    public void put(Data key, Object value) {
        fireTtlCleanup();
        if (evictionPolicy == NONE && cache.size() >= maxSize) {
            return;
        }
        if (evictionPolicy != NONE && cache.size() >= maxSize) {
            fireEvictCache();
        }
        if (value == null) {
            value = NULL_OBJECT;
        } else {
            value = inMemoryFormat.equals(InMemoryFormat.OBJECT)
                    ? serializationService.toObject(value) : serializationService.toData(value);
        }
        final NearCacheRecord record = new NearCacheRecord(key, value);
        NearCacheRecord previous = cache.put(key, record);
        updateSizeEstimator(calculateCost(record));
        if (previous != null) {
            updateSizeEstimator(-calculateCost(previous));
        }
    }

    @Override
    public NearCacheStatsImpl getNearCacheStats() {
        return createNearCacheStats();
    }

    @Override
    public Object selectToSave(Object... candidates) {
        throw new UnsupportedOperationException();
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
                            Set<NearCacheRecord> records = new TreeSet<NearCacheRecord>(selectedComparator);
                            records.addAll(cache.values());
                            int evictSize = (int) (cache.size() * EVICTION_FACTOR);
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

    @Override
    public String getName() {
        return mapName;
    }

    @Override
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

    @Override
    public boolean remove(Data key) {
        final NearCacheRecord record = cache.remove(key);
        // if a mapping exists for the key.
        if (record != null) {
            updateSizeEstimator(-calculateCost(record));
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isInvalidateOnChange() {
        return invalidateOnChange;
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public void clear() {
        cache.clear();
        resetSizeEstimator();
    }

    @Override
    public void destroy() {
        clear();
    }

    @Override
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    private void resetSizeEstimator() {
        getNearCacheSizeEstimator().reset();
    }

    private void updateSizeEstimator(long size) {
        getNearCacheSizeEstimator().add(size);
    }

    private long calculateCost(NearCacheRecord record) {
        return getNearCacheSizeEstimator().calculateSize(record);
    }

    public SizeEstimator getNearCacheSizeEstimator() {
        return nearCacheSizeEstimator;
    }

    public void setNearCacheSizeEstimator(SizeEstimator nearCacheSizeEstimator) {
        this.nearCacheSizeEstimator = nearCacheSizeEstimator;
    }
}
