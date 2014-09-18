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

import com.hazelcast.config.CacheConfig;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cache Partition Segment
 */
public final class CachePartitionSegment {

    private final NodeEngine nodeEngine;
    private final CacheService cacheService;
    private final int partitionId;
    private final ConstructorFunction<String, ICacheRecordStore> cacheConstructorFunction =
            new ConstructorFunction<String, ICacheRecordStore>() {
        public ICacheRecordStore createNew(String name) {
            return new CacheRecordStore(name, partitionId, nodeEngine, cacheService);
        }
    };
    private final ConcurrentMap<String, ICacheRecordStore> caches = new ConcurrentHashMap<String, ICacheRecordStore>();

    CachePartitionSegment(NodeEngine nodeEngine, CacheService cacheService, int partitionId) {
        this.nodeEngine = nodeEngine;
        this.cacheService = cacheService;
        this.partitionId = partitionId;
    }

    public Iterator<ICacheRecordStore> cacheIterator() {
        return caches.values().iterator();
    }

    public Collection<CacheConfig> getCacheConfigs() {
        return cacheService.getCacheConfigs();
    }

    int getPartitionId() {
        return partitionId;
    }

    ICacheRecordStore getOrCreateCache(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(caches, name, cacheConstructorFunction);
    }

    ICacheRecordStore getCache(String name) {
        return caches.get(name);
    }

    void deleteCache(String name) {
        ICacheRecordStore cache = caches.remove(name);
        if (cache != null) {
            cache.destroy();
        }
    }

    void clear() {
        for (String name : caches.keySet()) {
            deleteCache(name);
        }
        caches.clear();
    }

    void destroy() {
        clear();
    }

    boolean hasCache() {
        return !caches.isEmpty();
    }
}
