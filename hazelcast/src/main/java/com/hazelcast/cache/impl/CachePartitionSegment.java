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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheStorageType;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cache Partition Segment
 *
 * CachePartitionSegment is a data structure responsible from
 * all cache data of a partition defined by partitionId
 *
 * This Data structure is managed by CacheService
 *
 * @see com.hazelcast.cache.impl.CacheService
 *
 */
public final class CachePartitionSegment {

    private final ICacheService cacheService;
    private final int partitionId;
    // *** NOTE ***
    //      CacheInfo is implemented by considering comparison with String in hash-map.
    private final ConstructorFunction<CacheInfo, ICacheRecordStore> cacheConstructorFunction;
    private final ConcurrentMap<CacheInfo, ICacheRecordStore> caches =
            new ConcurrentHashMap<CacheInfo, ICacheRecordStore>();
    private final Object mutex = new Object();

    CachePartitionSegment(ICacheService cacheService,
                          ConstructorFunction<CacheInfo,
                          ICacheRecordStore> cacheConstructorFunction,
                          int partitionId) {
        this.cacheConstructorFunction = cacheConstructorFunction;
        this.cacheService = cacheService;
        this.partitionId = partitionId;
    }

    public Iterator<ICacheRecordStore> cacheIterator() {
        return caches.values().iterator();
    }

    public Collection<CacheConfig> getCacheConfigs() {
        return cacheService.getCacheConfigs();
    }

    public int getPartitionId() {
        return partitionId;
    }

    public ICacheRecordStore getOrCreateCache(String name) {
        return
            ConcurrencyUtil.getOrPutSynchronized(caches,
                                                 new CacheInfo(name),
                                                 mutex,
                                                 cacheConstructorFunction);
    }

    public ICacheRecordStore getOrCreateCache(String name, CacheStorageType cacheStorageType) {
        return
             ConcurrencyUtil.getOrPutSynchronized(caches,
                                                  new CacheInfo(name, cacheStorageType),
                                                  mutex,
                                                  cacheConstructorFunction);
    }

    public ICacheRecordStore getCache(String name) {
        return caches.get(name);
    }

    public void deleteCache(String name) {
        ICacheRecordStore cache = caches.remove(name);
        if (cache != null) {
            cache.destroy();
        }
    }

    public void clear() {
        synchronized (mutex) {
            for (ICacheRecordStore cache : caches.values()) {
                cache.destroy();
            }
        }
        caches.clear();
    }

    public void destroy() {
        clear();
    }

    public boolean hasAnyCache() {
        return !caches.isEmpty();
    }

    public boolean hasCache(String name) {
        return caches.containsKey(name);
    }
}
