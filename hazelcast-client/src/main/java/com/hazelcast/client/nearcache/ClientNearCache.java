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
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.ListenerSupport;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.client.MapAddEntryListenerRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.impl.PortableEntryEvent;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ali 7/18/13
 */
public class ClientNearCache {

    static final int evictionPercentage = 20;
    static final int cleanupInterval = 5000;
    final int maxSize;
    volatile long lastCleanup;
    final long maxIdleMillis;
    final long timeToLiveMillis;
    final boolean invalidateOnChange;
    final EvictionPolicy evictionPolicy;
    final InMemoryFormat inMemoryFormat;
    final String mapName;
    final ClientContext context;
    final AtomicBoolean canCleanUp;
    final AtomicBoolean canEvict;
    final ConcurrentMap<Data, CacheRecord> cache;
    ListenerSupport listenerSupport = null;
    public static final Object NULL_OBJECT = new Object();

    public ClientNearCache(String mapName, ClientContext context, NearCacheConfig nearCacheConfig) {
        this.mapName = mapName;
        this.context = context;
        maxSize = nearCacheConfig.getMaxSize();
        maxIdleMillis = nearCacheConfig.getMaxIdleSeconds() * 1000;
        inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        timeToLiveMillis = nearCacheConfig.getTimeToLiveSeconds() * 1000;
        invalidateOnChange = nearCacheConfig.isInvalidateOnChange();
        evictionPolicy = EvictionPolicy.valueOf(nearCacheConfig.getEvictionPolicy());
        cache = new ConcurrentHashMap<Data, CacheRecord>();
        canCleanUp = new AtomicBoolean(true);
        canEvict = new AtomicBoolean(true);
        lastCleanup = Clock.currentTimeMillis();
        if (invalidateOnChange) {
            addInvalidateListener();
        }
    }

    private void addInvalidateListener(){
        try {
            MapAddEntryListenerRequest request = new MapAddEntryListenerRequest(mapName, false);
            EventHandler<PortableEntryEvent> handler = new EventHandler<PortableEntryEvent>() {
                public void handle(PortableEntryEvent event) {
                    cache.remove(event.getKey());
                }
            };
            listenerSupport = new ListenerSupport(context, request, handler, null);
            listenerSupport.listen(new Callback() {
                public void notify(Object object) {
                    cache.clear();
                }
            });

        } catch (Exception e) {
            Logger.getLogger(ClientNearCache.class).severe("-----------------\n Near Cache is not initialized!!! \n-----------------", e);
        }

    }

    static enum EvictionPolicy {
        NONE, LRU, LFU
    }

    public void put(Data key, Object object) {
        fireTtlCleanup();
        if (evictionPolicy == EvictionPolicy.NONE && cache.size() >= maxSize) {
            return;
        }
        if (evictionPolicy != EvictionPolicy.NONE && cache.size() >= maxSize) {
            fireEvictCache();
        }
        Object value;
        if (object == null){
            value = NULL_OBJECT;
        } else {
            value = inMemoryFormat.equals(InMemoryFormat.BINARY) ? context.getSerializationService().toData(object) : object;
        }
        cache.put(key, new CacheRecord(key, value));
    }

    private void fireEvictCache() {
        if (canEvict.compareAndSet(true, false)) {
            try {
                context.getExecutionService().execute(new Runnable() {
                    public void run() {
                        try {
                            TreeSet<CacheRecord> records = new TreeSet<CacheRecord>(cache.values());
                            int evictSize = cache.size() * evictionPercentage / 100;
                            int i=0;
                            for (CacheRecord record : records) {
                                cache.remove(record.key);
                                if (++i > evictSize)
                                    break;
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
        if (Clock.currentTimeMillis() < (lastCleanup + cleanupInterval))
            return;

        if (canCleanUp.compareAndSet(true, false)) {
            try {
                context.getExecutionService().execute(new Runnable() {
                    public void run() {
                        try {
                            lastCleanup = Clock.currentTimeMillis();
                            for (Map.Entry<Data, CacheRecord> entry : cache.entrySet()) {
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

    public Object get(Data key) {
        fireTtlCleanup();
        CacheRecord record = cache.get(key);
        if (record != null) {
            record.access();
            if (record.expired()) {
                cache.remove(key);
                return null;
            }
            if (record.value.equals(NULL_OBJECT)){
                return NULL_OBJECT;
            }
            return inMemoryFormat.equals(InMemoryFormat.BINARY) ? context.getSerializationService().toObject((Data)record.value) : record.value;
        } else {
            return null;
        }
    }

    public void destroy() {
        if (listenerSupport != null){
            listenerSupport.stop();
        }
        cache.clear();
    }


    class CacheRecord implements Comparable<CacheRecord> {
        final Data key;
        final Object value;
        volatile long lastAccessTime;
        final long creationTime;
        final AtomicInteger hit;

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
            lastAccessTime = Clock.currentTimeMillis();
        }

        boolean expired() {
            long time = Clock.currentTimeMillis();
            return (maxIdleMillis > 0 && time > lastAccessTime + maxIdleMillis) || (timeToLiveMillis > 0 && time > creationTime + timeToLiveMillis);
        }

        public int compareTo(CacheRecord o) {
            if (EvictionPolicy.LRU.equals(evictionPolicy))
                return ((Long) this.lastAccessTime).compareTo((o.lastAccessTime));
            else if (EvictionPolicy.LFU.equals(evictionPolicy))
                return ((Integer) this.hit.get()).compareTo((o.hit.get()));

            return 0;
        }
    }
}
