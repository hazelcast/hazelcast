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

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;

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
        if (inMemoryFormat != BINARY && inMemoryFormat != OBJECT) {
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
        throw new UnsupportedOperationException();
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
                                NearCacheRecord nearCacheRecord = entry.getValue();

                                if (nearCacheRecord.getValue() == MARKER_VALUE) {
                                    continue;
                                }

                                if (nearCacheRecord.isExpired(maxIdleMillis, timeToLiveMillis)) {
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
            Object value = record.getValue();
            if (value == MARKER_VALUE) {
                return null;
            }
            record.access();
            if (record.isExpired(maxIdleMillis, timeToLiveMillis)) {
                cache.remove(key);
                stats.incrementMisses();
                return null;
            }
            if (value == NULL_OBJECT) {
                stats.incrementMisses();
                return NULL_OBJECT;
            }
            stats.incrementHits();
            return inMemoryFormat == BINARY ? context.getSerializationService().toObject(value) : value;
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
    public Object mapKeyToMarker(K key) {
        NearCacheRecord nearCacheRecord = new NearCacheRecord(key, MARKER_VALUE);
        cache.put(key, nearCacheRecord);
        return nearCacheRecord;
    }

    @Override
    public void updateKeyIfMappedToMarker(K key, Object marker, Object newValue) {
        fireTtlCleanup();
        if (evictionPolicy == NONE && cache.size() > maxSize) {
            cache.remove(key, marker);
            return;
        }
        if (evictionPolicy != NONE && cache.size() > maxSize) {
            fireEvictCache();
        }
        Object value = null;
        if (newValue != null) {
            SerializationService serializationService = context.getSerializationService();
            if (inMemoryFormat == BINARY) {
                value = serializationService.toData(newValue);
            } else if (inMemoryFormat == OBJECT) {
                value = serializationService.toObject(newValue);
            } else {
                throw new IllegalArgumentException();
            }
        }
        value = value == null ? NULL_OBJECT : value;

        NearCacheRecord nearCacheRecord = new NearCacheRecord(key, value);
        cache.replace(key, ((NearCacheRecord) marker), nearCacheRecord);
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
        cache.clear();
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
