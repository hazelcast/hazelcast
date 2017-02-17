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
    private NearCache<Object, Object> nearCache;
    private MapNearCacheManager mapNearCacheManager;
    private RepairingHandler repairingHandler;

    private volatile String invalidationListenerId;

    public NearCachedMapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine, MapConfig mapConfig) {
        super(name, mapService, nodeEngine, mapConfig);
    }

    @Override
    public void initialize() {
        super.initialize();

        mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        cacheLocalEntries = getMapConfig().getNearCacheConfig().isCacheLocalEntries();
        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();
        nearCache = mapNearCacheManager.getOrCreateNearCache(name, nearCacheConfig);
        invalidateOnChange = nearCache.isInvalidatedOnChange();

        if (invalidateOnChange) {
            addNearCacheInvalidateListener();
        }
    }

    // this operation returns the object in data format,
    // except when it is retrieved from Near Cache and Near Cache memory format is object
    @Override
    protected Object getInternal(Data key) {
        Object value = getCachedValue(key, true);
        if (value != NOT_CACHED) {
            return value;
        }

        long reservationId = tryReserveForUpdate(key);
        if (reservationId == NOT_RESERVED) {
            value = super.getInternal(key);
        } else {
            try {
                value = super.getInternal(key);
                value = tryPublishReserved(key, value, reservationId);
            } catch (Throwable throwable) {
                invalidateCache(key);
                throw rethrow(throwable);
            }
        }

        return value;
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
            invalidateCache(key);
            throw rethrow(t);
        }

        future.andThen(new ExecutionCallback<Data>() {
            @Override
            public void onResponse(Data value) {
                nearCache.tryPublishReserved(key, value, reservationId, false);
            }

            @Override
            public void onFailure(Throwable t) {
                invalidateCache(key);
            }
        });

        return future;
    }

    @Override
    protected Data putInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        Data data = super.putInternal(key, value, ttl, timeunit);
        invalidateCache(key);
        return data;
    }

    @Override
    protected boolean tryPutInternal(Data key, Data value, long timeout, TimeUnit timeunit) {
        boolean putInternal = super.tryPutInternal(key, value, timeout, timeunit);
        invalidateCache(key);
        return putInternal;
    }

    @Override
    protected Data putIfAbsentInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        Data data = super.putIfAbsentInternal(key, value, ttl, timeunit);
        invalidateCache(key);
        return data;
    }

    @Override
    protected void putTransientInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        invalidateCache(key);
        super.putTransientInternal(key, value, ttl, timeunit);
    }

    @Override
    protected InternalCompletableFuture<Data> putAsyncInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        InternalCompletableFuture<Data> future = super.putAsyncInternal(key, value, ttl, timeunit);
        invalidateCache(key);
        return future;
    }

    @Override
    protected InternalCompletableFuture<Data> setAsyncInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        InternalCompletableFuture<Data> future = super.setAsyncInternal(key, value, ttl, timeunit);
        invalidateCache(key);
        return future;
    }

    @Override
    protected boolean replaceInternal(Data key, Data expect, Data update) {
        boolean replaceInternal = super.replaceInternal(key, expect, update);
        invalidateCache(key);
        return replaceInternal;
    }

    @Override
    protected Data replaceInternal(Data key, Data value) {
        Data replaceInternal = super.replaceInternal(key, value);
        invalidateCache(key);
        return replaceInternal;
    }

    @Override
    protected void setInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        super.setInternal(key, value, ttl, timeunit);
        invalidateCache(key);
    }

    @Override
    protected boolean evictInternal(Data key) {
        boolean evictInternal = super.evictInternal(key);
        invalidateCache(key);
        return evictInternal;
    }

    @Override
    protected void evictAllInternal() {
        super.evictAllInternal();
        nearCache.clear();
    }

    @Override
    public void clearInternal() {
        super.clearInternal();
        nearCache.clear();
    }

    @Override
    public void loadAllInternal(boolean replaceExistingValues) {
        super.loadAllInternal(replaceExistingValues);
        if (replaceExistingValues) {
            nearCache.clear();
        }
    }

    @Override
    protected void loadInternal(Iterable<Data> keys, boolean replaceExistingValues) {
        super.loadInternal(keys, replaceExistingValues);
        invalidateCache(keys);
    }

    @Override
    protected Data removeInternal(Data key) {
        Data data = super.removeInternal(key);
        invalidateCache(key);
        return data;
    }

    @Override
    protected void removeAllInternal(Predicate predicate) {
        super.removeAllInternal(predicate);
        nearCache.clear();
    }

    @Override
    protected void deleteInternal(Data key) {
        super.deleteInternal(key);
        invalidateCache(key);
    }

    @Override
    protected boolean removeInternal(Data key, Data value) {
        boolean removeInternal = super.removeInternal(key, value);
        invalidateCache(key);
        return removeInternal;
    }

    @Override
    protected boolean tryRemoveInternal(Data key, long timeout, TimeUnit timeunit) {
        boolean removeInternal = super.tryRemoveInternal(key, timeout, timeunit);
        invalidateCache(key);
        return removeInternal;
    }

    @Override
    protected InternalCompletableFuture<Data> removeAsyncInternal(Data key) {
        invalidateCache(key);
        return super.removeAsyncInternal(key);
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
            invalidateCache(key);
        }
    }

    @Override
    protected void invokePutAllOperationFactory(Address address, long size, int[] partitions, MapEntries[] entries)
            throws Exception {
        super.invokePutAllOperationFactory(address, size, partitions, entries);
        for (MapEntries mapEntries : entries) {
            for (int i = 0; i < mapEntries.size(); i++) {
                invalidateCache(mapEntries.getKey(i));
            }
        }
    }

    @Override
    public Data executeOnKeyInternal(Data key, EntryProcessor entryProcessor) {
        Data data = super.executeOnKeyInternal(key, entryProcessor);
        invalidateCache(key);
        return data;
    }

    @Override
    public Map executeOnKeysInternal(Set<Data> keys, EntryProcessor entryProcessor) {
        Map map = super.executeOnKeysInternal(keys, entryProcessor);
        invalidateCache(keys);
        return map;
    }

    @Override
    public InternalCompletableFuture<Object> executeOnKeyInternal(Data key, EntryProcessor entryProcessor,
                                                                  ExecutionCallback<Object> callback) {
        InternalCompletableFuture<Object> future = super.executeOnKeyInternal(key, entryProcessor, callback);
        invalidateCache(key);
        return future;
    }

    @Override
    public void executeOnEntriesInternal(EntryProcessor entryProcessor, Predicate predicate,
                                         List<Data> resultingKeyValuePairs) {
        super.executeOnEntriesInternal(entryProcessor, predicate, resultingKeyValuePairs);

        for (int i = 0; i < resultingKeyValuePairs.size(); i += 2) {
            Data key = resultingKeyValuePairs.get(i);
            invalidateCache(key);
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

    protected void invalidateCache(Data key) {
        if (key == null) {
            return;
        }

        nearCache.remove(key);
    }

    protected void invalidateCache(Collection<Data> keys) {
        for (Data key : keys) {
            nearCache.remove(key);
        }
    }

    protected void invalidateCache(Iterable<Data> keys) {
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
