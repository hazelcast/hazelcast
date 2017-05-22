/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.HashSet;
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
public class CachePartitionSegment implements ConstructorFunction<String, ICacheRecordStore> {

    protected final AbstractCacheService cacheService;
    protected final int partitionId;
    protected final ConcurrentMap<String, ICacheRecordStore> recordStores =
            new ConcurrentHashMap<String, ICacheRecordStore>();
    protected final Object mutex = new Object();

    public CachePartitionSegment(final AbstractCacheService cacheService, final int partitionId) {
        this.cacheService = cacheService;
        this.partitionId = partitionId;
    }

    @Override public ICacheRecordStore createNew(String name) {
        return cacheService.createNewRecordStore(name, partitionId);
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
        return ConcurrencyUtil.getOrPutSynchronized(recordStores, name, mutex, this);
    }

    public ICacheRecordStore getRecordStore(String name) {
        return recordStores.get(name);
    }

    public ICacheService getCacheService() {
        return cacheService;
    }

    public void deleteRecordStore(String name, boolean destroy) {
        ICacheRecordStore store;
        if (destroy) {
            store = recordStores.remove(name);
            if (store != null) {
                store.destroy();
            }
        } else {
            store = recordStores.get(name);
            if (store != null) {
                store.close(false);
            }
        }
    }

    public boolean hasAnyRecordStore() {
        return !recordStores.isEmpty();
    }

    public boolean hasRecordStore(String name) {
        return recordStores.containsKey(name);
    }

    public void init() {
        synchronized (mutex) {
            for (ICacheRecordStore store : recordStores.values()) {
                store.init();
            }
        }
    }

    public void clear() {
        synchronized (mutex) {
            for (ICacheRecordStore store : recordStores.values()) {
                store.clear();
            }
        }
    }

    public void shutdown() {
        synchronized (mutex) {
            for (ICacheRecordStore store : recordStores.values()) {
                store.close(true);
            }
        }
        recordStores.clear();
    }

    void clearHavingLesserBackupCountThan(int backupCount) {
        synchronized (mutex) {
            for (ICacheRecordStore store : recordStores.values()) {
                CacheConfig cacheConfig = store.getConfig();
                if (backupCount > cacheConfig.getTotalBackupCount()) {
                    store.clear();
                }
            }
        }
    }

   public Collection<ServiceNamespace> getAllNamespaces(int replicaIndex) {
        Collection<ServiceNamespace> namespaces = new HashSet<ServiceNamespace>();
        for (ICacheRecordStore recordStore : recordStores.values()) {
            if (recordStore.getConfig().getTotalBackupCount() >= replicaIndex) {
                namespaces.add(recordStore.getObjectNamespace());
            }
        }
        return namespaces;
    }
}
