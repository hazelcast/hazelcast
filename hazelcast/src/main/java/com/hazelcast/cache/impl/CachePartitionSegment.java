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

package com.hazelcast.cache.impl;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * <p>
 *     Responsible for all cache data of a partition. Creates and
 * looks up {@link com.hazelcast.cache.impl.ICacheRecordStore CacheRecordStore}s by name.
 * </p>
 * A {@link CacheService} manages all <code>CachePartitionSegment</code>s.
 */
public final class CachePartitionSegment {

    private final AbstractCacheService cacheService;
    private final int partitionId;
    private final ConstructorFunction<String, ICacheRecordStore> storeConstructorFunction;
    private final ConcurrentMap<String, ICacheRecordStore> recordStores = new ConcurrentHashMap<String, ICacheRecordStore>();
    private final Object mutex = new Object();

    CachePartitionSegment(final AbstractCacheService cacheService, final int partitionId) {
        this.storeConstructorFunction = new ConstructorFunction<String, ICacheRecordStore>() {
            @Override
            public ICacheRecordStore createNew(String arg) {
                return cacheService.createNewRecordStore(arg, partitionId);
            }
        };
        this.cacheService = cacheService;
        this.partitionId = partitionId;
    }

    public Iterator<ICacheRecordStore> recordStoreIterator() {
        return recordStores.values().iterator();
    }

    public Collection<CacheConfig> getCacheConfigs() {
        return cacheService.getCacheConfigs();
    }

    public int getPartitionId() {
        return partitionId;
    }

    public ICacheRecordStore getOrCreateRecordStore(String name) {
        return ConcurrencyUtil.getOrPutSynchronized(recordStores, name, mutex, storeConstructorFunction);
    }

    public ICacheRecordStore getRecordStore(String name) {
        return recordStores.get(name);
    }

    public void deleteRecordStore(String name) {
        ICacheRecordStore store = recordStores.remove(name);
        if (store != null) {
            store.destroy();
        }
    }

    public void clear() {
        synchronized (mutex) {
            for (ICacheRecordStore store : recordStores.values()) {
                store.destroy();
            }
        }
        recordStores.clear();
    }

    public void destroy() {
        clear();
    }

    public boolean hasAnyRecordStore() {
        return !recordStores.isEmpty();
    }

    public boolean hasRecordStore(String name) {
        return recordStores.containsKey(name);
    }
}
