/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.proxy;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.internal.nearcache.impl.NearCacheDataFetcher;
import com.hazelcast.internal.nearcache.impl.invalidation.BatchNearCacheInvalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.nearcache.invalidation.InvalidationListener;
import com.hazelcast.map.impl.nearcache.invalidation.UuidFilter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A server-side {@code IMap} implementation which is fronted by a Near Cache.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class NearCachedMapProxyImpl<K, V> extends MapProxyImpl<K, V> implements NearCacheDataFetcher<V> {

    private final boolean serializeKeys;
    private final boolean cacheLocalEntries;
    private final boolean invalidateOnChange;
    private final ClusterService clusterService;

    private NearCache<K, V> nearCache;
    private RepairingHandler repairingHandler;
    private MapNearCacheManager mapNearCacheManager;

    private volatile String invalidationListenerId;

    public NearCachedMapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine, MapConfig mapConfig) {
        super(name, mapService, nodeEngine, mapConfig);

        clusterService = nodeEngine.getClusterService();

        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();
        cacheLocalEntries = nearCacheConfig.isCacheLocalEntries();
        invalidateOnChange = nearCacheConfig.isInvalidateOnChange();
        serializeKeys = nearCacheConfig.isSerializeKeys();
    }

    public NearCache<K, V> getNearCache() {
        return nearCache;
    }

    @Override
    public void initialize() {
        super.initialize();

        mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        nearCache = mapNearCacheManager.getOrCreateNearCache(name, mapConfig.getNearCacheConfig());
        nearCache.unwrap(DefaultNearCache.class).setNearCacheDataFetcher(this);
        if (invalidateOnChange) {
            registerInvalidationListener();
        }
    }

    @Override
    protected Object containsKey0(Object key) {
        return nearCache.containsKey((K) key);
    }

    @Override
    protected Object get0(Object key, @Nullable final Data keyData, final long startTimeNanos) {
        return nearCache.get((K) key);
    }

    @Override
    protected ICompletableFuture getAsync0(Object key, @Nullable final Data keyData, final long startTimeNanos) {
        return nearCache.getAsync((K) key);
    }

    @Override
    protected void getAll0(Collection<K> keys, Map<K, V> result, final long startTimeNanos) {
        nearCache.getAll(keys, result, startTimeNanos);
    }

    @Override
    public void interceptAfterGet(String name, V val) {
        mapServiceContext.interceptAfterGet(name, val);
    }

    @Override
    protected Data putInternal(Object key, Data value, long ttl,
                               TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.putInternal(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean setTtlInternal(Object key, long ttl, TimeUnit timeUnit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.setTtlInternal(key, ttl, timeUnit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean tryPutInternal(Object key, Data value, long timeout, TimeUnit timeunit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.tryPutInternal(key, value, timeout, timeunit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected Data putIfAbsentInternal(Object key, Data value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.putIfAbsentInternal(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected void putTransientInternal(Object key, Data value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            super.putTransientInternal(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected InternalCompletableFuture<Data> putAsyncInternal(Object key, Data value, long ttl, TimeUnit ttlUnit,
                                                               long maxIdle, TimeUnit maxIdleUnit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.putAsyncInternal(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected InternalCompletableFuture<Data> setAsyncInternal(Object key, Data value, long ttl, TimeUnit ttlUnit,
                                                               long maxIdle, TimeUnit maxIdleUnit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.setAsyncInternal(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean replaceInternal(Object key, Data expect, Data update) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.replaceInternal(key, expect, update);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected Data replaceInternal(Object key, Data value) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.replaceInternal(key, value);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected void setInternal(Object key, Data value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            super.setInternal(key, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean evictInternal(Object key) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.evictInternal(key);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected void evictAllInternal() {
        try {
            super.evictAllInternal();
        } finally {
            nearCache.clear();
        }
    }

    @Override
    public void clearInternal() {
        try {
            super.clearInternal();
        } finally {
            nearCache.clear();
        }
    }

    @Override
    public void loadAllInternal(boolean replaceExistingValues) {
        try {
            super.loadAllInternal(replaceExistingValues);
        } finally {
            if (replaceExistingValues) {
                nearCache.clear();
            }
        }
    }

    @Override
    protected void loadInternal(Set<K> keys, Iterable<Data> dataKeys, boolean replaceExistingValues) {
        if (serializeKeys) {
            dataKeys = convertToData(keys);
        }
        try {
            super.loadInternal(keys, dataKeys, replaceExistingValues);
        } finally {
            Iterable<?> ncKeys = serializeKeys ? dataKeys : keys;
            for (Object key : ncKeys) {
                invalidateNearCache(key);
            }
        }
    }

    @Override
    protected Data removeInternal(Object key) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.removeInternal(key);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected void removeAllInternal(Predicate predicate) {
        try {
            super.removeAllInternal(predicate);
        } finally {
            nearCache.clear();
        }
    }

    @Override
    protected void deleteInternal(Object key) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            super.deleteInternal(key);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean removeInternal(Object key, Data value) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.removeInternal(key, value);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean tryRemoveInternal(Object key, long timeout, TimeUnit timeunit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.tryRemoveInternal(key, timeout, timeunit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected InternalCompletableFuture<Data> removeAsyncInternal(Object key) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.removeAsyncInternal(key);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected void invokePutAllOperationFactory(long size, int[] partitions, MapEntries[] entries) throws Exception {
        try {
            super.invokePutAllOperationFactory(size, partitions, entries);
        } finally {
            if (serializeKeys) {
                for (MapEntries mapEntries : entries) {
                    if (mapEntries != null) {
                        for (int i = 0; i < mapEntries.size(); i++) {
                            invalidateNearCache(mapEntries.getKey(i));
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void finalizePutAll(Map<?, ?> map) {
        try {
            super.finalizePutAll(map);
        } finally {
            if (!serializeKeys) {
                for (Object key : map.keySet()) {
                    invalidateNearCache(key);
                }
            }
        }
    }

    @Override
    public Data executeOnKeyInternal(Object key, EntryProcessor entryProcessor) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.executeOnKeyInternal(key, entryProcessor);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    public ICompletableFuture<Map<K, Object>> submitToKeysInternal(Set<K> keys, Set<Data> dataKeys,
                                                                   EntryProcessor entryProcessor) {
        if (serializeKeys) {
            toDataCollectionWithNonNullKeyValidation(keys, dataKeys);
        }
        try {
            return super.submitToKeysInternal(keys, dataKeys, entryProcessor);
        } finally {
            Set<?> ncKeys = serializeKeys ? dataKeys : keys;
            for (Object key : ncKeys) {
                invalidateNearCache(key);
            }
        }
    }

    @Override
    public InternalCompletableFuture<Object> executeOnKeyInternal(Object key, EntryProcessor entryProcessor,
                                                                  ExecutionCallback<Object> callback) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.executeOnKeyInternal(key, entryProcessor, callback);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    public void executeOnEntriesInternal(EntryProcessor entryProcessor, Predicate predicate, List<Data> resultingKeyValuePairs) {
        try {
            super.executeOnEntriesInternal(entryProcessor, predicate, resultingKeyValuePairs);
        } finally {
            for (int i = 0; i < resultingKeyValuePairs.size(); i += 2) {
                Data key = resultingKeyValuePairs.get(i);
                invalidateNearCache(serializeKeys ? key : toObject(key));
            }
        }
    }

    @Override
    protected void postDestroy() {
        try {
            if (invalidateOnChange) {
                mapNearCacheManager.deregisterRepairingHandler(name);
                removeEntryListener(invalidationListenerId);
            }
        } finally {
            super.postDestroy();
        }
    }

    protected void invalidateNearCache(Object ncKey) {
        if (ncKey == null) {
            return;
        }
        nearCache.invalidate((K) ncKey);
    }

    private boolean cachingAllowedFor(Data keyData) {
        return cacheLocalEntries || clusterService.getLocalMember().isLiteMember() || !isOwn(keyData);
    }

    private boolean isOwn(Data key) {
        int partitionId = partitionService.getPartitionId(key);
        return partitionService.isPartitionOwner(partitionId);
    }

    private Object toNearCacheKeyWithStrategy(@Nonnull Object key) {
        return serializeKeys ? ss.toData(key, partitionStrategy) : key;
    }

    public String addNearCacheInvalidationListener(InvalidationListener listener) {
        // local member UUID may change after a split-brain merge
        String localMemberUuid = getNodeEngine().getClusterService().getLocalMember().getUuid();
        EventFilter eventFilter = new UuidFilter(localMemberUuid);
        return mapServiceContext.addEventListener(listener, eventFilter, name);
    }

    private void registerInvalidationListener() {
        repairingHandler = mapNearCacheManager.newRepairingHandler(name, nearCache);
        invalidationListenerId = addNearCacheInvalidationListener(new NearCacheInvalidationListener());
    }

    @Override
    public V getWithoutNearCaching(Object toDataWithStrategy, Object param, long startTimeNanos) {
        return (V) toObject(super.getInternal(toDataWithStrategy));
    }

    @Override
    public ICompletableFuture getAsyncWithoutNearCaching(Object key, Object param, long startTimeNanos) {
        return super.getAsyncInternal(toObject(key));
    }

    @Override
    public InternalCompletableFuture invokeGetOperation(Data keyData, Data param,
                                                        boolean asyncGet, long startTimeNanos) {
        return super.invokeGetOperation(keyData, asyncGet, startTimeNanos);
    }

    @Override
    public InternalCompletableFuture invokeGetAllOperation(Collection<Data> dataKeys, Data param,
                                                           long startTimeNanos) {
        return super.invokeGetAllOperation(dataKeys, startTimeNanos);
    }

    @Override
    public InternalCompletableFuture<Boolean> invokeContainsKeyOperation(Object key) {
        return super.invokeContainsKeyOperation(key);
    }

    @Override
    public boolean isStatsEnabled() {
        return statisticsEnabled;
    }

    private final class NearCacheInvalidationListener implements InvalidationListener {

        @Override
        public void onInvalidate(Invalidation invalidation) {
            assert invalidation != null;

            if (invalidation instanceof BatchNearCacheInvalidation) {
                List<Invalidation> batch = ((BatchNearCacheInvalidation) invalidation).getInvalidations();
                for (Invalidation single : batch) {
                    handleInternal(single);
                }
            } else {
                handleInternal(invalidation);
            }
        }

        private void handleInternal(Invalidation single) {
            repairingHandler.handle(single.getKey(), single.getSourceUuid(), single.getPartitionUuid(), single.getSequence());
        }
    }
}
