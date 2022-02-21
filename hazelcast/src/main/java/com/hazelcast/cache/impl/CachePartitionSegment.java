/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.eviction.ExpirationManager;
import com.hazelcast.internal.partition.impl.NameSpaceUtil;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

/**
 * <p>
 * Responsible for all cache data of a partition. Creates and
 * looks up {@link com.hazelcast.cache.impl.ICacheRecordStore CacheRecordStore}s by name.
 * </p>
 * A {@link CacheService} manages all <code>CachePartitionSegment</code>s.
 */
public class CachePartitionSegment implements ConstructorFunction<String, ICacheRecordStore> {

    protected final int partitionId;
    protected final Object mutex = new Object();
    protected final AbstractCacheService cacheService;
    protected final ConcurrentMap<String, ICacheRecordStore> recordStores = new ConcurrentHashMap<>();
    private boolean runningCleanupOperation;

    private volatile long lastCleanupTime;

    /**
     * Used when sorting partition containers in {@link ExpirationManager}
     * A non-volatile copy of lastCleanupTime is used with two reasons.
     * <p>
     * 1. We need an un-modified field during sorting.
     * 2. Decrease number of volatile reads.
     */
    private long lastCleanupTimeCopy;

    public CachePartitionSegment(final AbstractCacheService cacheService, final int partitionId) {
        this.cacheService = cacheService;
        this.partitionId = partitionId;
    }

    @Override
    public ICacheRecordStore createNew(String cacheNameWithPrefix) {
        return cacheService.createNewRecordStore(cacheNameWithPrefix, partitionId);
    }

    public Iterator<ICacheRecordStore> recordStoreIterator() {
        return recordStores.values().iterator();
    }

    public boolean hasRunningCleanupOperation() {
        return runningCleanupOperation;
    }

    public void setRunningCleanupOperation(boolean status) {
        runningCleanupOperation = status;
    }

    public long getLastCleanupTime() {
        return lastCleanupTime;
    }

    public void setLastCleanupTime(long time) {
        lastCleanupTime = time;
    }

    public long getLastCleanupTimeBeforeSorting() {
        return lastCleanupTimeCopy;
    }

    public void storeLastCleanupTime() {
        lastCleanupTimeCopy = getLastCleanupTime();
    }

    public Collection<CacheConfig> getCacheConfigs() {
        return cacheService.getCacheConfigs();
    }

    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Gets or creates a cache record store with the prefixed {@code cacheNameWithPrefix}.
     *
     * @param cacheNameWithPrefix the full name of the {@link com.hazelcast.cache.ICache}, including the manager scope prefix
     * @return the cache partition record store
     */
    public ICacheRecordStore getOrCreateRecordStore(String cacheNameWithPrefix) {
        return ConcurrencyUtil.getOrPutSynchronized(recordStores, cacheNameWithPrefix, mutex, this);
    }

    /**
     * Returns a cache record store with the prefixed
     * {@code cacheNameWithPrefix} or {@code null} if one doesn't exist.
     *
     * @param cacheNameWithPrefix the full name of the {@link com.hazelcast.cache.ICache},
     *                            including the manager scope prefix
     * @return the cache partition record store or {@code null} if it doesn't exist
     */
    public ICacheRecordStore getRecordStore(String cacheNameWithPrefix) {
        return recordStores.get(cacheNameWithPrefix);
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

    public void reset() {
        synchronized (mutex) {
            for (ICacheRecordStore store : recordStores.values()) {
                store.reset();
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
                    store.reset();
                }
            }
        }
    }

    public Collection<ServiceNamespace> getAllNamespaces(int replicaIndex) {
        return getNamespaces(ignored -> true, replicaIndex);
    }

    public Collection<ServiceNamespace> getNamespaces(Predicate<CacheConfig> predicate, int replicaIndex) {
        return NameSpaceUtil.getAllNamespaces(recordStores,
                recordStore -> {
                    CacheConfig cacheConfig = recordStore.getConfig();
                    return recordStore.getConfig().getTotalBackupCount() >= replicaIndex
                            && predicate.test(cacheConfig);
                }, ICacheRecordStore::getObjectNamespace);
    }
}
