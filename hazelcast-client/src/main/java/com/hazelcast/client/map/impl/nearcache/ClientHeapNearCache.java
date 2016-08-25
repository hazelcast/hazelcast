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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.impl.nearcache.NearCacheRecord;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
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

/**
 * Implementation of the {@link NearCache}.
 * <p/>
 * todo: improve javadoc.
 *
 * @param <K>
 */
public class ClientHeapNearCache<K> implements NearCache<K, Object> {

    /**
     * Eviction factor
     */
    private static final double EVICTION_FACTOR = 0.2;

    /**
     * TTL Clean up interval
     */
    private static final int TTL_CLEANUP_INTERVAL_MILLS = 5000;

    private final int maxSize;
    private final long maxIdleMillis;
    private final long timeToLiveMillis;
    private final boolean invalidateOnChange;
    private final EvictionPolicy evictionPolicy;
    private final InMemoryFormat inMemoryFormat;
    private final String mapName;
    private final ClientContext context;
    private final AtomicBoolean canCleanUp;
    private final AtomicBoolean canEvict;
    private final ConcurrentMap<K, NearCacheRecord> cache;
    private final NearCacheStatsImpl stats;
    private final Comparator<NearCacheRecord> selectedComparator;

    private volatile long lastCleanup;

    public ClientHeapNearCache(String mapName, ClientContext context, NearCacheConfig nearCacheConfig) {
        this.mapName = mapName;
        this.context = context;
        maxSize = nearCacheConfig.getMaxSize();
        maxIdleMillis = TimeUnit.SECONDS.toMillis(nearCacheConfig.getMaxIdleSeconds());
        inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat != InMemoryFormat.BINARY && inMemoryFormat != InMemoryFormat.OBJECT) {
            throw new IllegalArgumentException("Illegal in-memory-format: " + inMemoryFormat);
        }
        timeToLiveMillis = TimeUnit.SECONDS.toMillis(nearCacheConfig.getTimeToLiveSeconds());
        invalidateOnChange = nearCacheConfig.isInvalidateOnChange();
        evictionPolicy = EvictionPolicy.valueOf(nearCacheConfig.getEvictionPolicy());
        selectedComparator = NearCacheRecord.getComparator(evictionPolicy);
        cache = new ConcurrentHashMap<K, NearCacheRecord>();
        canCleanUp = new AtomicBoolean(true);
        canEvict = new AtomicBoolean(true);
        lastCleanup = Clock.currentTimeMillis();
        stats = new NearCacheStatsImpl();
    }

    @Override
    public void put(K key, Object object) {
        fireTtlCleanup();
        if (evictionPolicy == EvictionPolicy.NONE && cache.size() >= maxSize) {
            return;
        }
        if (evictionPolicy != EvictionPolicy.NONE && cache.size() >= maxSize) {
            fireEvictCache();
        }
        Object value = null;
        if (object != null) {
            SerializationService serializationService = context.getSerializationService();
            if (inMemoryFormat == InMemoryFormat.BINARY) {
                value = serializationService.toData(object);
            } else if (inMemoryFormat == InMemoryFormat.OBJECT) {
                value = serializationService.toObject(object);
            } else {
                throw new IllegalArgumentException();
            }
        }
        value = value == null ? NULL_OBJECT : value;
        cache.put(key, new NearCacheRecord(key, value));
    }

    private void fireEvictCache() {
        if (canEvict.compareAndSet(true, false)) {
            try {
                final ClientExecutionServiceImpl executionService = (ClientExecutionServiceImpl) context.getExecutionService();
                executionService.executeInternal(new Runnable() {
                    public void run() {
                        try {
                            Set<NearCacheRecord> records = new TreeSet<NearCacheRecord>(selectedComparator);
                            records.addAll(cache.values());
                            int evictSize = (int) (cache.size() * EVICTION_FACTOR);
                            int i = 0;
                            for (NearCacheRecord record : records) {
                                cache.remove(record.getKey());
                                if (++i > evictSize) {
                                    break;
                                }
                            }
                        } finally {
                            canEvict.set(true);
                        }
                        if (cache.size() >= maxSize && canEvict.compareAndSet(true, false)) {
                            try {
                                executionService.executeInternal(this);
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
        if (Clock.currentTimeMillis() < (lastCleanup + TTL_CLEANUP_INTERVAL_MILLS)) {
            return;
        }

        if (canCleanUp.compareAndSet(true, false)) {
            try {
                ClientExecutionServiceImpl executionService = (ClientExecutionServiceImpl) context.getExecutionService();
                executionService.executeInternal(new Runnable() {
                    public void run() {
                        try {
                            lastCleanup = Clock.currentTimeMillis();
                            for (Map.Entry<K, NearCacheRecord> entry : cache.entrySet()) {
                                if (entry.getValue().isExpired(maxIdleMillis, timeToLiveMillis)) {
                                    cache.remove(entry.getKey());
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
    public Object get(K key) {
        fireTtlCleanup();
        NearCacheRecord record = cache.get(key);
        if (record != null) {
            if (record.isExpired(maxIdleMillis, timeToLiveMillis)) {
                cache.remove(key);
                stats.incrementMisses();
                return null;
            }
            if (record.getValue().equals(NULL_OBJECT)) {
                stats.incrementMisses();
                return NULL_OBJECT;
            }
            stats.incrementHits();
            record.access();
            return inMemoryFormat.equals(InMemoryFormat.BINARY)
                    ? context.getSerializationService().toObject(record.getValue()) : record.getValue();
        } else {
            stats.incrementMisses();
            return null;
        }
    }

    @Override
    public boolean remove(K key) {
        return null != cache.remove(key);
    }

    @Override
    public NearCacheStatsImpl getNearCacheStats() {
        long ownedEntryCount = 0;
        long ownedEntryMemory = 0;
        for (NearCacheRecord record : cache.values()) {
            ownedEntryCount++;
            ownedEntryMemory += record.getCost();
        }
        stats.setOwnedEntryCount(ownedEntryCount);
        stats.setOwnedEntryMemoryCost(ownedEntryMemory);
        return stats;
    }

    @Override
    public Object selectToSave(Object... candidates) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public void destroy() {
        clear();
    }

    @Override
    public boolean isInvalidateOnChange() {
        return invalidateOnChange;
    }

    @Override
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

}
