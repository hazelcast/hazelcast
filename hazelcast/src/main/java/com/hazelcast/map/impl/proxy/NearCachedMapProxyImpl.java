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

package com.hazelcast.map.impl.proxy;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.impl.invalidation.BatchNearCacheInvalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.InterceptorRegistry;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.nearcache.invalidation.InvalidationListener;
import com.hazelcast.map.impl.nearcache.invalidation.UuidFilter;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventFilter;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.nearcache.NearCache.UpdateSemantic.READ_UPDATE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

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

    private volatile UUID invalidationListenerId;

    public NearCachedMapProxyImpl(String name, MapService mapService,
                                  NodeEngine nodeEngine, MapConfig mapConfig) {
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
        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();
        nearCache = mapNearCacheManager.getOrCreateNearCache(name, nearCacheConfig);
        if (invalidateOnChange) {
            registerInvalidationListener();
        }
    }

    // this operation returns the value as Data, except when it's retrieved from Near Cache with in-memory-format OBJECT
    @Override
    @SuppressWarnings("unchecked")
    protected V getInternal(Object key) {
        key = toNearCacheKeyWithStrategy(key);
        V value = (V) getCachedValue(key, true);
        if (value != NOT_CACHED) {
            return value;
        }

        try {
            Data keyData = toDataWithStrategy(key);
            long reservationId = tryReserveForUpdate(key, keyData);
            value = (V) super.getInternal(keyData);
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
        final Object ncKey = toNearCacheKeyWithStrategy(key);
        Object value = getCachedValue(ncKey, false);
        if (value != NOT_CACHED) {
            return InternalCompletableFuture.newCompletedFuture(value);
        }

        final Data keyData = toDataWithStrategy(ncKey);
        final long reservationId = tryReserveForUpdate(ncKey, keyData);
        InternalCompletableFuture<Data> future;
        try {
            future = super.getAsyncInternal(keyData);
        } catch (Throwable t) {
            invalidateNearCache(ncKey);
            throw rethrow(t);
        }

        if (reservationId != NOT_RESERVED) {
            future.whenCompleteAsync((v, t) -> {
                if (t == null) {
                    nearCache.tryPublishReserved(ncKey, v, reservationId, false);
                } else {
                    invalidateNearCache(ncKey);
                }
            });
        }
        return future;
    }

    @Override
    protected Data putInternal(Object key, Data valueData, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.putInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
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
    protected InternalCompletableFuture<Data> putAsyncInternal(Object key, Data valueData, long ttl, TimeUnit ttlUnit,
                                                               long maxIdle, TimeUnit maxIdleUnit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.putAsyncInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected InternalCompletableFuture<Data> putIfAbsentAsyncInternal(Object key, Data valueData,
                                                                       long ttl, TimeUnit ttlUnit,
                                                                       long maxIdle, TimeUnit maxIdleUnit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.putIfAbsentAsyncInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
        } finally {
            // If the operation is retried, the return value can be wrong.
            // Consider: key k isn't present. We do putIfAbsent(k, v). The operation puts the value, updates
            // the backup, but before the response is sent, the member crashes. The caller doesn't receive the response,
            // so retries with the new key owner. This time, the operation does nothing because the key isn't absent,
            // and returns the old value.
            //The unnecessary invalidation doesn't hurt as much as non-invalidated near cache would.
            invalidateNearCache(key);
        }
    }

    @Override
    protected InternalCompletableFuture<Data> setAsyncInternal(Object key, Data valueData, long ttl, TimeUnit ttlUnit,
                                                               long maxIdle, TimeUnit maxIdleUnit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            return super.setAsyncInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
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
    protected void setInternal(Object key, Data valueData, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        key = toNearCacheKeyWithStrategy(key);
        try {
            super.setInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
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
    protected boolean containsKeyInternal(Object key) {
        key = toNearCacheKeyWithStrategy(key);
        Object cachedValue = getCachedValue(key, false);
        if (cachedValue != NOT_CACHED) {
            return cachedValue != null;
        }
        return super.containsKeyInternal(key);
    }

    @Override
    protected void getAllInternal(Set<K> keys, List<Data> dataKeys, List<Object> resultingKeyValuePairs) {
        if (serializeKeys) {
            toDataKeysWithReservations(keys, dataKeys, null, null);
        }
        Collection<?> ncKeys = serializeKeys ? dataKeys : new LinkedList<>(keys);

        populateResultFromNearCache(ncKeys, resultingKeyValuePairs);
        if (ncKeys.isEmpty()) {
            return;
        }

        Map<Object, Long> reservations = createHashMap(ncKeys.size());
        Map<Data, Object> reverseKeyMap = null;
        if (!serializeKeys) {
            reverseKeyMap = createHashMap(ncKeys.size());
            toDataKeysWithReservations(ncKeys, dataKeys, reservations, reverseKeyMap);
        } else {
            //noinspection unchecked
            createNearCacheReservations((Collection<Data>) ncKeys, reservations);
        }

        try {
            int currentSize = resultingKeyValuePairs.size();
            super.getAllInternal(keys, dataKeys, resultingKeyValuePairs);
            populateResultFromRemote(currentSize, resultingKeyValuePairs, reservations, reverseKeyMap);
        } finally {
            releaseReservedKeys(reservations);
        }
    }

    private void toDataKeysWithReservations(Collection<?> keys, Collection<Data> dataKeys, Map<Object, Long> reservations,
                                            Map<Data, Object> reverseKeyMap) {
        for (Object key : keys) {
            Data keyData = toDataWithStrategy(key);
            dataKeys.add(keyData);
            if (reservations != null) {
                long reservationId = tryReserveForUpdate(key, keyData);
                if (reservationId != NOT_RESERVED) {
                    reservations.put(key, reservationId);
                }
            }
            if (reverseKeyMap != null) {
                reverseKeyMap.put(keyData, key);
            }
        }
    }

    private void populateResultFromNearCache(Collection keys, List<Object> resultingKeyValuePairs) {
        Iterator iterator = keys.iterator();
        while (iterator.hasNext()) {
            Object key = iterator.next();
            Object value = getCachedValue(key, true);
            if (value != null && value != NOT_CACHED) {
                resultingKeyValuePairs.add(key);
                resultingKeyValuePairs.add(value);
                iterator.remove();
            }
        }
    }

    private void createNearCacheReservations(Collection<Data> dataKeys, Map<Object, Long> reservations) {
        for (Data key : dataKeys) {
            long reservationId = tryReserveForUpdate(key, key);
            if (reservationId != NOT_RESERVED) {
                reservations.put(key, reservationId);
            }
        }
    }

    private void populateResultFromRemote(int currentSize, List<Object> resultingKeyValuePairs, Map<Object, Long> reservations,
                                          Map<Data, Object> reverseKeyMap) {
        for (int i = currentSize; i < resultingKeyValuePairs.size(); i += 2) {
            Data keyData = (Data) resultingKeyValuePairs.get(i);
            Data valueData = (Data) resultingKeyValuePairs.get(i + 1);

            Object ncKey = serializeKeys ? keyData : reverseKeyMap.get(keyData);
            if (!serializeKeys) {
                resultingKeyValuePairs.set(i, ncKey);
            }

            Long reservationId = reservations.get(ncKey);
            if (reservationId != null) {
                Object cachedValue = tryPublishReserved(ncKey, valueData, reservationId);
                resultingKeyValuePairs.set(i + 1, cachedValue);
                reservations.remove(ncKey);
            }
        }
    }

    private void releaseReservedKeys(Map<Object, Long> reservationResults) {
        for (Object key : reservationResults.keySet()) {
            invalidateNearCache(key);
        }
    }

    @Override
    protected void putAllVisitSerializedKeys(MapEntries[] entries) {
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

    @Override
    protected void finalizePutAll(Map<?, ?> map) {
        if (!serializeKeys) {
            for (Object key : map.keySet()) {
                invalidateNearCache(key);
            }
        }
    }

    @Override
    public InternalCompletableFuture<Data> executeOnKeyInternal(Object key, EntryProcessor entryProcessor) {
        final Object nearCacheKey = toNearCacheKeyWithStrategy(key);
        try {
            return super.executeOnKeyInternal(nearCacheKey, entryProcessor);
        } finally {
            invalidateNearCache(nearCacheKey);
        }
    }

    @Override
    public <R> InternalCompletableFuture<Map<K, R>> submitToKeysInternal(Set<K> keys,
                                                                         Set<Data> dataKeys,
                                                                         EntryProcessor<K, V, R> entryProcessor) {
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

    protected void invalidateNearCache(Object key) {
        if (key == null) {
            return;
        }
        nearCache.invalidate(key);
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
        InterceptorRegistry interceptorRegistry = mapServiceContext.getMapContainer(name).getInterceptorRegistry();
        mapServiceContext.interceptAfterGet(interceptorRegistry, value);
        return deserializeValue ? toObject(value) : value;
    }

    private long tryReserveForUpdate(Object key, Data keyData) {
        if (!cachingAllowedFor(keyData)) {
            return NOT_RESERVED;
        }
        return nearCache.tryReserveForUpdate(key, keyData, READ_UPDATE);
    }

    private boolean cachingAllowedFor(Data keyData) {
        return cacheLocalEntries || clusterService.getLocalMember().isLiteMember() || !isOwn(keyData);
    }

    private boolean isOwn(Data key) {
        int partitionId = partitionService.getPartitionId(key);
        return partitionService.isPartitionOwner(partitionId);
    }

    private Object toNearCacheKeyWithStrategy(Object key) {
        return serializeKeys ? serializationService.toData(key, partitionStrategy) : key;
    }

    public UUID addNearCacheInvalidationListener(InvalidationListener listener) {
        // local member UUID may change after a split-brain merge
        UUID localMemberUuid = getNodeEngine().getClusterService().getLocalMember().getUuid();
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
