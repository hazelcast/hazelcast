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

import com.hazelcast.client.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.client.ClientRequest;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.client.MapAddEntryListenerRequest;
import com.hazelcast.map.client.MapRemoveEntryListenerRequest;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.PortableEntryEvent;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ClientNearCache
 *
 * @param <K> key type
 */
public class ClientNearCache<K> {

    /**
     * Used when caching nonexistent values.
     */
    public static final Object NULL_OBJECT = new Object();
    private static final double EVICTION_PERCENTAGE = 0.2;
    private final ClientNearCacheType cacheType;
    private final int maxSize;
    private volatile long lastCleanup;
    private final long maxIdleMillis;
    private final long timeToLiveMillis;
    private final EvictionPolicy evictionPolicy;
    private final InMemoryFormat inMemoryFormat;
    private final String mapName;
    private final ClientContext context;
    private final AtomicBoolean canCleanUp;
    private final AtomicBoolean canEvict;
    private final ConcurrentMap<K, CacheRecord<K>> cache;
    private final NearCacheStatsImpl clientNearCacheStats;
    private String registrationId;

    private final Comparator<CacheRecord<K>> comparator = new Comparator<CacheRecord<K>>() {
        public int compare(CacheRecord<K> o1, CacheRecord<K> o2) {
            if (EvictionPolicy.LRU.equals(evictionPolicy)) {
                return ((Long) o1.lastAccessTime).compareTo((o2.lastAccessTime));
            } else if (EvictionPolicy.LFU.equals(evictionPolicy)) {
                return ((Integer) o1.hit.get()).compareTo((o2.hit.get()));
            }

            return 0;
        }
    };


    public ClientNearCache(String mapName, ClientNearCacheType cacheType,
                           ClientContext context, NearCacheConfig nearCacheConfig) {
        this.mapName = mapName;
        this.cacheType = cacheType;
        this.context = context;
        maxSize = nearCacheConfig.getMaxSize();
        maxIdleMillis = TimeUnit.SECONDS.toMillis(nearCacheConfig.getMaxIdleSeconds());
        inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        timeToLiveMillis = TimeUnit.SECONDS.toMillis(nearCacheConfig.getTimeToLiveSeconds());
        boolean invalidateOnChange = nearCacheConfig.isInvalidateOnChange();
        evictionPolicy = EvictionPolicy.valueOf(nearCacheConfig.getEvictionPolicy());
        cache = new ConcurrentHashMap<K, CacheRecord<K>>();
        canCleanUp = new AtomicBoolean(true);
        canEvict = new AtomicBoolean(true);
        lastCleanup = Clock.currentTimeMillis();
        clientNearCacheStats = new NearCacheStatsImpl();
        if (invalidateOnChange) {
            addInvalidateListener();
        }
    }

    private void addInvalidateListener() {
        try {
            ClientRequest request;
            EventHandler handler;
            if (cacheType == ClientNearCacheType.Map) {
                request = new MapAddEntryListenerRequest(mapName, false);
                handler = new EventHandler<PortableEntryEvent>() {
                    public void handle(PortableEntryEvent event) {
                        switch (event.getEventType()) {
                            case ADDED:
                            case REMOVED:
                            case UPDATED:
                            case EVICTED:
                                final Data key = event.getKey();
                                cache.remove(key);
                                break;
                            case CLEAR_ALL:
                            case EVICT_ALL:
                                cache.clear();
                                break;
                            default:
                                throw new IllegalArgumentException("Not a known event type " + event.getEventType());
                        }
                    }

                    @Override
                    public void onListenerRegister() {
                        cache.clear();
                    }
                };
            } else {
                throw new IllegalStateException("Near cache is not available for this type of data structure");
            }
            //TODO callback
            registrationId = context.getListenerService().listen(request, null, handler);
        } catch (Exception e) {
            Logger.getLogger(ClientNearCache.class).
                    severe("-----------------\n Near Cache is not initialized!!! \n-----------------", e);
        }

    }

    static enum EvictionPolicy {
        NONE, LRU, LFU
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
            value = inMemoryFormat.equals(InMemoryFormat.BINARY) ? context.getSerializationService().toData(object) : object;
        }
        cache.put(key, new CacheRecord<K>(key, value));
    }

    private void fireEvictCache() {
        if (canEvict.compareAndSet(true, false)) {
            try {
                context.getExecutionService().execute(new Runnable() {
                    public void run() {
                        try {
                            TreeSet<CacheRecord<K>> records = new TreeSet<CacheRecord<K>>(comparator);
                            records.addAll(cache.values());
                            int evictSize = (int) (EVICTION_PERCENTAGE * cache.size());
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
        if (Clock.currentTimeMillis() < (lastCleanup + timeToLiveMillis)) {
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

    public void invalidate() {
        cache.clear();
    }

    public void invalidate(K key) {
        cache.remove(key);
    }

    public void invalidate(Collection<K> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        for (K key : keys) {
            cache.remove(key);
        }
    }

    public Object get(K key) {
        fireTtlCleanup();
        CacheRecord<K> record = cache.get(key);
        if (record != null) {
            if (record.expired()) {
                cache.remove(key);
                clientNearCacheStats.incrementMisses();
                return null;
            }
            if (record.value.equals(NULL_OBJECT)) {
                clientNearCacheStats.incrementMisses();
                return NULL_OBJECT;
            }
            record.access();
            return inMemoryFormat.equals(InMemoryFormat.BINARY) ? context.getSerializationService().
                    toObject((Data) record.value) : record.value;
        } else {
            clientNearCacheStats.incrementMisses();
            return null;
        }
    }

    public NearCacheStatsImpl getNearCacheStats() {
        return createNearCacheStats();
    }

    private NearCacheStatsImpl createNearCacheStats() {
        long ownedEntryCount = cache.values().size();
        long ownedEntryMemory = 0;
        for (CacheRecord record : cache.values()) {
            ownedEntryMemory += record.getCost();
        }
        clientNearCacheStats.setOwnedEntryCount(ownedEntryCount);
        clientNearCacheStats.setOwnedEntryMemoryCost(ownedEntryMemory);
        return clientNearCacheStats;
    }

    public void destroy() {
        if (registrationId != null) {
            BaseClientRemoveListenerRequest request;
            if (cacheType == ClientNearCacheType.Map) {
                request = new MapRemoveEntryListenerRequest(mapName, registrationId);
            } else {
                throw new IllegalStateException("Near cache is not available for this type of data structure");
            }
            context.getListenerService().stopListening(request, registrationId);
        }
        cache.clear();
    }

    public void clear() {
        cache.clear();
    }

    class CacheRecord<K> {
        final K key;
        final Object value;
        volatile long lastAccessTime;
        final long creationTime;
        final AtomicInteger hit;

        CacheRecord(K key, Object value) {
            this.key = key;
            this.value = value;
            long time = Clock.currentTimeMillis();
            this.lastAccessTime = time;
            this.creationTime = time;
            this.hit = new AtomicInteger(0);
        }

        void access() {
            hit.incrementAndGet();
            clientNearCacheStats.incrementHits();
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
                    // sizeof atomic integer
                    + (Integer.SIZE / Byte.SIZE)
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
