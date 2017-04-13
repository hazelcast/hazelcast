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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.EventFilter;
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

    private boolean cacheLocalEntries;
    private boolean invalidateOnChange;

    private MapNearCacheManager mapNearCacheManager;
    private NearCache<Object, Object> nearCache;
    private RepairingHandler repairingHandler;

    private volatile String invalidationListenerId;

    public NearCachedMapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine, MapConfig mapConfig) {
        super(name, mapService, nodeEngine, mapConfig);
    }

    @Override
    public void initialize() {
        super.initialize();

        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();
        cacheLocalEntries = nearCacheConfig.isCacheLocalEntries();
        invalidateOnChange = nearCacheConfig.isInvalidateOnChange();

        mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        nearCache = mapNearCacheManager.getOrCreateNearCache(name, nearCacheConfig);
        if (invalidateOnChange) {
            addNearCacheInvalidateListener();
        }
    }

    // this operation returns the object in data format,
    // except when it is retrieved from Near Cache and Near Cache memory format is object
    @Override
    @SuppressWarnings("unchecked")
    protected V getInternal(Data key) {
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
    protected InternalCompletableFuture<Data> getAsyncInternal(final Data key) {
        Object value = getCachedValue(key, false);
        if (value != NOT_CACHED) {
            return new CompletedFuture<Data>(
                    getNodeEngine().getSerializationService(),
                    value,
                    getNodeEngine().getExecutionService().getExecutor(ASYNC_EXECUTOR));
        }

        final long reservationId = tryReserveForUpdate(key);
        InternalCompletableFuture<Data> future;
        try {
            future = super.getAsyncInternal(key);
        } catch (Throwable t) {
            invalidateNearCache(key);
            throw rethrow(t);
        }

        if (reservationId != NOT_RESERVED) {
            future.andThen(new ExecutionCallback<Data>() {
                @Override
                public void onResponse(Data value) {
                    nearCache.tryPublishReserved(key, value, reservationId, false);
                }

                @Override
                public void onFailure(Throwable t) {
                    invalidateNearCache(key);
                }
            });
        }

        return future;
    }

    @Override
    protected Data putInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        Data data;
        try {
            data = super.putInternal(key, value, ttl, timeunit);
        } finally {
            invalidateNearCache(key);
        }
        return data;
    }

    @Override
    protected boolean tryPutInternal(Data key, Data value, long timeout, TimeUnit timeunit) {
        boolean putInternal;
        try {
            putInternal = super.tryPutInternal(key, value, timeout, timeunit);
        } finally {
            invalidateNearCache(key);
        }
        return putInternal;
    }

    @Override
    protected Data putIfAbsentInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        Data data;
        try {
            data = super.putIfAbsentInternal(key, value, ttl, timeunit);
        } finally {
            invalidateNearCache(key);
        }
        return data;
    }

    @Override
    protected void putTransientInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        try {
            super.putTransientInternal(key, value, ttl, timeunit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected InternalCompletableFuture<Data> putAsyncInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        InternalCompletableFuture<Data> future;
        try {
            future = super.putAsyncInternal(key, value, ttl, timeunit);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    protected InternalCompletableFuture<Data> setAsyncInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        InternalCompletableFuture<Data> future;
        try {
            future = super.setAsyncInternal(key, value, ttl, timeunit);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    protected boolean replaceInternal(Data key, Data expect, Data update) {
        boolean replaceInternal;
        try {
            replaceInternal = super.replaceInternal(key, expect, update);
        } finally {
            invalidateNearCache(key);
        }
        return replaceInternal;
    }

    @Override
    protected Data replaceInternal(Data key, Data value) {
        Data replaceInternal;
        try {
            replaceInternal = super.replaceInternal(key, value);
        } finally {
            invalidateNearCache(key);
        }
        return replaceInternal;
    }

    @Override
    protected void setInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        try {
            super.setInternal(key, value, ttl, timeunit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean evictInternal(Data key) {
        boolean evictInternal;
        try {
            evictInternal = super.evictInternal(key);
        } finally {
            invalidateNearCache(key);
        }
        return evictInternal;
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
    protected void loadInternal(Iterable<Data> keys, boolean replaceExistingValues) {
        try {
            super.loadInternal(keys, replaceExistingValues);
        } finally {
            invalidateNearCache(keys);
        }
    }

    @Override
    protected Data removeInternal(Data key) {
        Data data;
        try {
            data = super.removeInternal(key);
        } finally {
            invalidateNearCache(key);
        }
        return data;
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
    protected void deleteInternal(Data key) {
        try {
            super.deleteInternal(key);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean removeInternal(Data key, Data value) {
        boolean removeInternal;
        try {
            removeInternal = super.removeInternal(key, value);
        } finally {
            invalidateNearCache(key);
        }
        return removeInternal;
    }

    @Override
    protected boolean tryRemoveInternal(Data key, long timeout, TimeUnit timeunit) {
        boolean removeInternal;
        try {
            removeInternal = super.tryRemoveInternal(key, timeout, timeunit);
        } finally {
            invalidateNearCache(key);
        }
        return removeInternal;
    }

    @Override
    protected InternalCompletableFuture<Data> removeAsyncInternal(Data key) {
        InternalCompletableFuture<Data> future;
        try {
            future = super.removeAsyncInternal(key);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    protected boolean containsKeyInternal(Data keyData) {
        Object cachedValue = getCachedValue(keyData, false);
        if (cachedValue != NOT_CACHED) {
            return true;
        }

        return super.containsKeyInternal(keyData);
    }

    @Override
    protected void getAllObjectInternal(List<Data> keys, List<Object> resultingKeyValuePairs) {
        getCachedValues(keys, resultingKeyValuePairs);

        Map<Data, Long> reservations = emptyMap();
        try {
            reservations = tryReserveForUpdate(keys);
            int currentSize = resultingKeyValuePairs.size();

            super.getAllObjectInternal(keys, resultingKeyValuePairs);

            for (int i = currentSize; i < resultingKeyValuePairs.size(); ) {
                Data key = toData(resultingKeyValuePairs.get(i++));
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

    private Map<Data, Long> tryReserveForUpdate(List<Data> keys) {
        Map<Data, Long> reservedKeys = createHashMap(keys.size());
        for (Data key : keys) {
            long reservationId = tryReserveForUpdate(key);
            if (reservationId != NOT_RESERVED) {
                reservedKeys.put(key, reservationId);
            }
        }

        return reservedKeys;
    }

    private void releaseReservedKeys(Map<Data, Long> reservationResults) {
        for (Data key : reservationResults.keySet()) {
            invalidateNearCache(key);
        }
    }

    @Override
    protected void invokePutAllOperationFactory(Address address, long size,
                                                int[] partitions, MapEntries[] entries) throws Exception {
        try {
            super.invokePutAllOperationFactory(address, size, partitions, entries);
        } finally {
            for (MapEntries mapEntries : entries) {
                for (int i = 0; i < mapEntries.size(); i++) {
                    invalidateNearCache(mapEntries.getKey(i));
                }
            }
        }
    }

    @Override
    public Data executeOnKeyInternal(Data key, EntryProcessor entryProcessor) {
        Data data;
        try {
            data = super.executeOnKeyInternal(key, entryProcessor);
        } finally {
            invalidateNearCache(key);
        }
        return data;
    }

    @Override
    public Map executeOnKeysInternal(Set<Data> keys, EntryProcessor entryProcessor) {
        Map map;
        try {
            map = super.executeOnKeysInternal(keys, entryProcessor);
        } finally {
            invalidateNearCache(keys);
        }
        return map;
    }

    @Override
    public InternalCompletableFuture<Object> executeOnKeyInternal(Data key, EntryProcessor entryProcessor,
                                                                  ExecutionCallback<Object> callback) {
        InternalCompletableFuture<Object> future;
        try {
            future = super.executeOnKeyInternal(key, entryProcessor, callback);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    public void executeOnEntriesInternal(EntryProcessor entryProcessor, Predicate predicate,
                                         List<Data> resultingKeyValuePairs) {
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

    private void getCachedValues(List<Data> keys, List<Object> resultingKeyValuePairs) {
        Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            Data key = iterator.next();
            Object value = getCachedValue(key, true);
            if (value == null || value == NOT_CACHED) {
                continue;
            }
            resultingKeyValuePairs.add(toObject(key));
            resultingKeyValuePairs.add(toObject(value));

            iterator.remove();
        }
    }

    protected void invalidateNearCache(Data key) {
        if (key == null) {
            return;
        }

        nearCache.remove(key);
    }

    protected void invalidateNearCache(Collection<Data> keys) {
        for (Data key : keys) {
            nearCache.remove(key);
        }
    }

    protected void invalidateNearCache(Iterable<Data> keys) {
        for (Data key : keys) {
            nearCache.remove(key);
        }
    }

    protected boolean isOwn(Data key) {
        int partitionId = partitionService.getPartitionId(key);
        Address partitionOwner = partitionService.getPartitionOwner(partitionId);
        return thisAddress.equals(partitionOwner);
    }

    private Object tryPublishReserved(Data key, Object value, long reservationId) {
        assert value != NOT_CACHED;

        // `value` is cached even it is null.
        Object cachedValue = nearCache.tryPublishReserved(key, value, reservationId, true);
        return cachedValue != null ? cachedValue : value;
    }

    private boolean cachingAllowedFor(Data key) {
        return !isOwn(key) || cacheLocalEntries;
    }

    private Object getCachedValue(Data key, boolean deserializeValue) {
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

    private long tryReserveForUpdate(Data key) {
        if (!cachingAllowedFor(key)) {
            return NOT_RESERVED;
        }

        return nearCache.tryReserveForUpdate(key);
    }

    public NearCache<Object, Object> getNearCache() {
        return nearCache;
    }

    private void addNearCacheInvalidateListener() {
        repairingHandler = mapNearCacheManager.newRepairingHandler(name, nearCache);
        EventFilter eventFilter = new UuidFilter(getNodeEngine().getLocalMember().getUuid());
        invalidationListenerId = mapServiceContext.addEventListener(new NearCacheInvalidationListener(), eventFilter, name);
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
