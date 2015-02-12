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

package com.hazelcast.client.nearcache;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.QuickMath;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
    final ConcurrentMap<K, CacheRecord<K>> cache;
    final NearCacheStatsImpl stats;
    private final Comparator<CacheRecord<K>> selectedComparator;

    private volatile long lastCleanup;
    private volatile String id;

    private final Comparator<CacheRecord<K>> lruComparator = new Comparator<CacheRecord<K>>() {
        public int compare(CacheRecord<K> o1, CacheRecord<K> o2) {
            final int result = QuickMath.compareLongs(o1.lastAccessTime, o2.lastAccessTime);
            if (result != 0) {
                return result;
            }
            return QuickMath.compareIntegers(o1.key.hashCode(), o2.key.hashCode());
        }
    };

    private final Comparator<CacheRecord<K>> lfuComparator = new Comparator<CacheRecord<K>>() {
        public int compare(CacheRecord<K> o1, CacheRecord<K> o2) {
            final int result = QuickMath.compareLongs(o1.hit.get(), o2.hit.get());
            if (result != 0) {
                return result;
            }
            return QuickMath.compareIntegers(o1.key.hashCode(), o2.key.hashCode());
        }
    };

    private final Comparator<CacheRecord<K>> defaultComparator = new Comparator<CacheRecord<K>>() {
        public int compare(CacheRecord<K> o1, CacheRecord<K> o2) {
            return QuickMath.compareIntegers(o1.key.hashCode(), o2.key.hashCode());
        }
    };

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
        if (EvictionPolicy.LRU.equals(evictionPolicy)) {
            selectedComparator = lruComparator;
        } else if (EvictionPolicy.LFU.equals(evictionPolicy)) {
            selectedComparator = lfuComparator;
        } else {
            selectedComparator = defaultComparator;
        }
        cache = new ConcurrentHashMap<K, CacheRecord<K>>();
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
        cache.put(key, new CacheRecord<K>(key, value));
    }

    private void fireEvictCache() {
        if (canEvict.compareAndSet(true, false)) {
            try {
                final ClientExecutionService executionService = context.getExecutionService();
                executionService.execute(new Runnable() {
                    public void run() {
                        try {
                            TreeSet<CacheRecord<K>> records = new TreeSet<CacheRecord<K>>(selectedComparator);
                            records.addAll(cache.values());
                            int evictSize = cache.size() * EVICTION_PERCENTAGE / HUNDRED_PERCENTAGE;
                            int i = 0;
                            for (CacheRecord<K> record : records) {
                                cache.remove(record.key);
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
                            for (Map.Entry<K, CacheRecord<K>> entry : cache.entrySet()) {
                                if (entry.getValue().expired()) {
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
        CacheRecord<K> record = cache.get(key);
        if (record != null) {
            record.access();
            if (record.expired()) {
                cache.remove(key);
                stats.incrementMisses();
                return null;
            }
            if (record.value.equals(NULL_OBJECT)) {
                stats.incrementMisses();
                return NULL_OBJECT;
            }
            stats.incrementHits();
            return inMemoryFormat.equals(InMemoryFormat.BINARY)
                    ? context.getSerializationService().toObject(record.value) : record.value;
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
        for (CacheRecord record : cache.values()) {
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

    private class CacheRecord<K> {
        final K key;
        final Object value;
        final long creationTime;
        final AtomicLong hit;
        volatile long lastAccessTime;

        CacheRecord(K key, Object value) {
            this.key = key;
            this.value = value;
            long time = Clock.currentTimeMillis();
            this.lastAccessTime = time;
            this.creationTime = time;
            this.hit = new AtomicLong();
        }

        void access() {
            hit.incrementAndGet();
            lastAccessTime = Clock.currentTimeMillis();
        }

        public long getCost() {
            // todo find object size  if not a Data instance.
            if (!(value instanceof Data)) {
                return 0;
            }
            if (!(key instanceof Data)) {
                return 0;
            }
            // value is Data
            return ((Data) key).getHeapCost()
                    + ((Data) value).getHeapCost()
                    + 2 * (Long.SIZE / Byte.SIZE)
                    // sizeof atomic long
                    + (Long.SIZE / Byte.SIZE)
                    // object references (key, value, hit)
                    + 3 * (Integer.SIZE / Byte.SIZE);
        }

        boolean expired() {
            long time = Clock.currentTimeMillis();
            return (maxIdleMillis > 0 && time > lastAccessTime + maxIdleMillis)
                    || (timeToLiveMillis > 0 && time > creationTime + timeToLiveMillis);
        }

    }
}
