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

package com.hazelcast.map.impl.proxy;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nearcache.NearCache;
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
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.createHashMap;
import static java.util.Collections.emptyMap;

/**
 * A server-side {@code IMap} implementation which is fronted by a Near Cache.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class NearCachedMapProxyImpl<K, V> extends MapProxyImpl<K, V> {

    private final ClusterService clusterService;
    private final boolean cacheLocalEntries;
    private final boolean invalidateOnChange;
    private final boolean serializeKeys;

    private MapNearCacheManager mapNearCacheManager;
    private NearCache<Object, Object> nearCache;
    private RepairingHandler repairingHandler;

    private volatile String invalidationListenerId;

    public NearCachedMapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine, MapConfig mapConfig) {
        super(name, mapService, nodeEngine, mapConfig);

        clusterService = nodeEngine.getClusterService();

        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();
        cacheLocalEntries = nearCacheConfig.isCacheLocalEntries();
        invalidateOnChange = nearCacheConfig.isInvalidateOnChange();
        serializeKeys = nearCacheConfig.isSerializeKeys();
    }

    public NearCache<Object, Object> getNearCache() {
        return nearCache;
    }

    @Override
    public void initialize() {
        super.initialize();

        mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        nearCache = mapNearCacheManager.getOrCreateNearCache(name, mapConfig.getNearCacheConfig());
        if (invalidateOnChange) {
            registerInvalidationListener();
        }
    }

    // this operation returns the value as Data, except when it's retrieved from Near Cache with in-memory-format OBJECT
    @Override
    @SuppressWarnings("unchecked")
    protected V getInternal(Object key) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        V value = (V) getCachedValue(key, true);
        if (value != NOT_CACHED) {
            return value;
        }

        try {
            long reservationId = tryReserveForUpdate(key);
            value = (V) super.getInternal(key);
            if (reservationId != NOT_RESERVED) {
                value = (V) tryPublishReserved(key, value, reservationId);
            }
            return value;
        } catch (Throwable throwable) {
            invalidateNearCache(key);
            throw rethrow(throwable);
        }
    }

    @Override
    protected InternalCompletableFuture<Data> getAsyncInternal(Object key) {
        final Object ncKey = serializeKeys ? toDataWithStrategy(key) : key;
        Object value = getCachedValue(ncKey, false);
        if (value != NOT_CACHED) {
            ExecutionService executionService = getNodeEngine().getExecutionService();
            return new CompletedFuture<Data>(serializationService, value, executionService.getExecutor(ASYNC_EXECUTOR));
        }

        final long reservationId = tryReserveForUpdate(ncKey);
        InternalCompletableFuture<Data> future;
        try {
            future = super.getAsyncInternal(ncKey);
        } catch (Throwable t) {
            invalidateNearCache(ncKey);
            throw rethrow(t);
        }

        if (reservationId != NOT_RESERVED) {
            future.andThen(new ExecutionCallback<Data>() {
                @Override
                public void onResponse(Data value) {
                    nearCache.tryPublishReserved(ncKey, value, reservationId, false);
                }

                @Override
                public void onFailure(Throwable t) {
                    invalidateNearCache(ncKey);
                }
            });
        }
        return future;
    }

    @Override
    protected Data putInternal(Object key, Data value, long ttl, TimeUnit timeunit) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            return super.putInternal(key, value, ttl, timeunit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean tryPutInternal(Object key, Data value, long timeout, TimeUnit timeunit) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            return super.tryPutInternal(key, value, timeout, timeunit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected Data putIfAbsentInternal(Object key, Data value, long ttl, TimeUnit timeunit) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            return super.putIfAbsentInternal(key, value, ttl, timeunit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected void putTransientInternal(Object key, Data value, long ttl, TimeUnit timeunit) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            super.putTransientInternal(key, value, ttl, timeunit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected InternalCompletableFuture<Data> putAsyncInternal(Object key, Data value, long ttl, TimeUnit timeunit) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            return super.putAsyncInternal(key, value, ttl, timeunit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected InternalCompletableFuture<Data> setAsyncInternal(Object key, Data value, long ttl, TimeUnit timeunit) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            return super.setAsyncInternal(key, value, ttl, timeunit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean replaceInternal(Object key, Data expect, Data update) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            return super.replaceInternal(key, expect, update);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected Data replaceInternal(Object key, Data value) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            return super.replaceInternal(key, value);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected void setInternal(Object key, Data value, long ttl, TimeUnit timeunit) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            super.setInternal(key, value, ttl, timeunit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean evictInternal(Object key) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
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
            for (Object key : serializeKeys ? dataKeys : keys) {
                invalidateNearCache(key);
            }
        }
    }

    @Override
    protected Data removeInternal(Object key) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
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
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            super.deleteInternal(key);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean removeInternal(Object key, Data value) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            return super.removeInternal(key, value);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean tryRemoveInternal(Object key, long timeout, TimeUnit timeunit) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            return super.tryRemoveInternal(key, timeout, timeunit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected InternalCompletableFuture<Data> removeAsyncInternal(Object key) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            return super.removeAsyncInternal(key);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean containsKeyInternal(Object key) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
        Object cachedValue = getCachedValue(key, false);
        if (cachedValue != NOT_CACHED) {
            return true;
        }
        return super.containsKeyInternal(key);
    }

    @Override
    protected void getAllObjectInternal(Set<K> keys, List<Data> dataKeys, List<Object> resultingKeyValuePairs) {
        if (serializeKeys) {
            toDataCollection(keys, dataKeys);
        }
        Collection ncKeys = serializeKeys ? dataKeys : keys;

        getCachedValues(ncKeys, resultingKeyValuePairs);

        Map<Object, Long> reservations = emptyMap();
        try {
            reservations = tryReserveForUpdate(ncKeys);
            int currentSize = resultingKeyValuePairs.size();

            super.getAllObjectInternal(keys, dataKeys, resultingKeyValuePairs);

            for (int i = currentSize; i < resultingKeyValuePairs.size(); ) {
                Object key = resultingKeyValuePairs.get(i++);
                if (serializeKeys) {
                    key = toDataWithStrategy(key);
                }
                Data value = toData(resultingKeyValuePairs.get(i++));

                Long reservationId = reservations.get(key);
                if (reservationId != null) {
                    Object cachedValue = tryPublishReserved(key, value, reservationId);
                    resultingKeyValuePairs.set(i - 1, cachedValue);
                    reservations.remove(key);
                }
            }
        } finally {
            releaseReservedKeys(reservations);
        }
    }

    private Map<Object, Long> tryReserveForUpdate(Collection keys) {
        Map<Object, Long> reservedKeys = createHashMap(keys.size());
        for (Object key : keys) {
            long reservationId = tryReserveForUpdate(key);
            if (reservationId != NOT_RESERVED) {
                reservedKeys.put(key, reservationId);
            }
        }
        return reservedKeys;
    }

    private void releaseReservedKeys(Map<Object, Long> reservationResults) {
        for (Object key : reservationResults.keySet()) {
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
        key = serializeKeys ? toDataWithStrategy(key) : key;
        try {
            return super.executeOnKeyInternal(key, entryProcessor);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    public Map<K, Object> executeOnKeysInternal(Set<K> keys, Set<Data> dataKeys, EntryProcessor entryProcessor) {
        if (serializeKeys) {
            toDataCollection(keys, dataKeys);
        }
        try {
            return super.executeOnKeysInternal(keys, dataKeys, entryProcessor);
        } finally {
            for (Object key : serializeKeys ? dataKeys : keys) {
                invalidateNearCache(key);
            }
        }
    }

    @Override
    public InternalCompletableFuture<Object> executeOnKeyInternal(Object key, EntryProcessor entryProcessor,
                                                                  ExecutionCallback<Object> callback) {
        key = serializeKeys ? toDataWithStrategy(key) : key;
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
                invalidateNearCache(key);
            }
        }
    }

    @Override
    protected boolean preDestroy() {
        if (invalidateOnChange) {
            mapNearCacheManager.deregisterRepairingHandler(name);
            removeEntryListener(invalidationListenerId);
        }
        return super.preDestroy();
    }

    private void getCachedValues(Collection keys, List<Object> resultingKeyValuePairs) {
        Iterator iterator = keys.iterator();
        while (iterator.hasNext()) {
            Object key = iterator.next();
            Object value = getCachedValue(key, true);
            if (value == null || value == NOT_CACHED) {
                continue;
            }
            resultingKeyValuePairs.add(toObject(key));
            resultingKeyValuePairs.add(toObject(value));

            iterator.remove();
        }
    }

    protected void invalidateNearCache(Object key) {
        if (key == null) {
            return;
        }
        nearCache.remove(key);
    }

    private Object tryPublishReserved(Object key, Object value, long reservationId) {
        assert value != NOT_CACHED;

        // `value` is cached even if it's null
        Object cachedValue = nearCache.tryPublishReserved(key, value, reservationId, true);
        return cachedValue != null ? cachedValue : value;
    }

    private Object getCachedValue(Object key, boolean deserializeValue) {
        Object value = nearCache.get(key);
        if (value == null) {
            return NOT_CACHED;
        }
        if (value == CACHED_AS_NULL) {
            return null;
        }
        mapServiceContext.interceptAfterGet(name, value);
        return deserializeValue ? toObject(value) : value;
    }

    private long tryReserveForUpdate(Object key) {
        if (!cachingAllowedFor(key)) {
            return NOT_RESERVED;
        }
        return nearCache.tryReserveForUpdate(key);
    }

    private boolean cachingAllowedFor(Object key) {
        return cacheLocalEntries || clusterService.getLocalMember().isLiteMember() || !isOwn(key);
    }

    private boolean isOwn(Object key) {
        key = serializeKeys ? key : toDataWithStrategy(key);
        int partitionId = partitionService.getPartitionId(key);
        return partitionService.isPartitionOwner(partitionId);
    }

    public String addNearCacheInvalidationListener(InvalidationListener listener) {
        // local member uuid may change after a split-brain merge
        String localMemberUuid = getNodeEngine().getClusterService().getLocalMember().getUuid();
        EventFilter eventFilter = new UuidFilter(localMemberUuid);
        return mapServiceContext.addEventListener(listener, eventFilter, name);
    }

    private void registerInvalidationListener() {
        repairingHandler = mapNearCacheManager.newRepairingHandler(name, nearCache);
        invalidationListenerId = addNearCacheInvalidationListener(new NearCacheInvalidationListener());
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
