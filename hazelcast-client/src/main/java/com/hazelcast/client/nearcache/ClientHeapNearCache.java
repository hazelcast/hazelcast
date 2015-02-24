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

package com.hazelcast.client.nearcache;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.impl.NearCacheRecord;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of the {@link com.hazelcast.client.nearcache.ClientNearCache}.
 * <p/>
 * todo: improve javadoc.
 *
 * @param <K>
 */
public class ClientHeapNearCache<K>
        implements ClientNearCache<K, Object> {

    private static final int HUNDRED_PERCENTAGE = 100;

    final int maxSize;
    final long maxIdleMillis;
    final long timeToLiveMillis;
    final boolean invalidateOnChange;
    final EvictionPolicy evictionPolicy;
    final InMemoryFormat inMemoryFormat;
    final String mapName;
    final ClientContext context;
    final AtomicBoolean canCleanUp;
    final AtomicBoolean canEvict;
    final ConcurrentMap<K, NearCacheRecord> cache;
    final NearCacheStatsImpl stats;
    private final Comparator<NearCacheRecord> selectedComparator;

    private volatile long lastCleanup;
    private volatile String id;


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

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void put(K key, Object object) {
        fireTtlCleanup();
        if (evictionPolicy == EvictionPolicy.NONE && cache.size() >= maxSize) {
            return;
        }
        if (evictionPolicy != EvictionPolicy.NONE && cache.size() >= maxSize) {
            fireEvictCache();
        }
        Object value;
        if (object == null) {
            value = NULL_OBJECT;
        } else {
            SerializationService serializationService = context.getSerializationService();
            if (inMemoryFormat == InMemoryFormat.BINARY) {
                value = serializationService.toData(object);
            } else if (inMemoryFormat == InMemoryFormat.OBJECT) {
                value = serializationService.toObject(object);
            } else {
                throw new IllegalArgumentException();
            }
        }
        cache.put(key, new NearCacheRecord(key, value));
    }

    private void fireEvictCache() {
        if (canEvict.compareAndSet(true, false)) {
            try {
                final ClientExecutionService executionService = context.getExecutionService();
                executionService.execute(new Runnable() {
                    public void run() {
                        try {
                            TreeSet<NearCacheRecord> records = new TreeSet<NearCacheRecord>(selectedComparator);
                            records.addAll(cache.values());
                            int evictSize = cache.size() * EVICTION_PERCENTAGE / HUNDRED_PERCENTAGE;
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
                                executionService.execute(this);
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
                context.getExecutionService().execute(new Runnable() {
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

    public Object get(K key) {
        fireTtlCleanup();
        NearCacheRecord record = cache.get(key);
        if (record != null) {
            record.access();
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
            return inMemoryFormat.equals(InMemoryFormat.BINARY)
                    ? context.getSerializationService().toObject(record.getValue()) : record.getValue();
        } else {
            stats.incrementMisses();
            return null;
        }
    }

    public void remove(K key) {
        cache.remove(key);
    }

    public void invalidate(K key) {
        cache.remove(key);
    }

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

    public void clear() {
        cache.clear();
    }

    public void destroy() {
        cache.clear();
    }

    @Override
    public boolean isInvalidateOnChange() {
        return invalidateOnChange;
    }

    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

}
