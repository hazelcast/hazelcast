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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveEntryListenerCodec;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.adapter.IMapDataStructureAdapter;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.nearcache.NearCache.UpdateSemantic.READ_UPDATE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static java.util.Collections.emptyMap;

/**
 * A Client-side {@code IMap} implementation which is fronted by a Near Cache.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class NearCachedClientMapProxy<K, V> extends ClientMapProxy<K, V> {

    private boolean serializeKeys;
    private NearCache<Object, Object> nearCache;

    private volatile UUID invalidationListenerId;

    public NearCachedClientMapProxy(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        NearCacheConfig nearCacheConfig = getContext().getClientConfig().getNearCacheConfig(name);
        serializeKeys = nearCacheConfig.isSerializeKeys();

        NearCacheManager nearCacheManager = getContext().getNearCacheManager(getServiceName());
        nearCache = nearCacheManager.getOrCreateNearCache(name, nearCacheConfig);

        if (nearCacheConfig.isInvalidateOnChange()) {
            registerInvalidationListener();
        }

        if (nearCacheConfig.getPreloaderConfig().isEnabled()) {
            nearCacheManager.startPreloading(nearCache, new IMapDataStructureAdapter<>(this));
        }
    }

    @Override
    protected boolean containsKeyInternal(Object key) {
        key = toNearCacheKey(key);
        Object cached = getCachedValue(key, false);
        if (cached != NOT_CACHED) {
            return cached != null;
        }
        return super.containsKeyInternal(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V getInternal(Object key) {
        key = toNearCacheKey(key);
        V value = (V) getCachedValue(key, true);
        if (value != NOT_CACHED) {
            return value;
        }

        try {
            Data keyData = toData(key);
            long reservationId = nearCache.tryReserveForUpdate(key, keyData, READ_UPDATE);
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
    protected boolean setTtlInternal(Object key, long ttl, TimeUnit timeUnit) {
        key = toNearCacheKey(key);
        try {
            return super.setTtlInternal(key, ttl, timeUnit);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    public InternalCompletableFuture<V> getAsync(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Object ncKey = toNearCacheKey(key);
        Object value = getCachedValue(ncKey, false);
        if (value != NOT_CACHED) {
            return newCompletedFuture(value, getSerializationService());
        }

        Data keyData = toData(ncKey);
        final long reservationId = nearCache.tryReserveForUpdate(ncKey, keyData, READ_UPDATE);
        ClientInvocationFuture invocationFuture;
        try {
            invocationFuture = super.getAsyncInternal(keyData);
        } catch (Throwable t) {
            invalidateNearCache(ncKey);
            throw rethrow(t);
        }

        if (reservationId != NOT_RESERVED) {
            invocationFuture.whenCompleteAsync((response, t) -> {
                if (t == null) {
                    Object newDecodedResponse = MapGetCodec.decodeResponse(response);
                    nearCache.tryPublishReserved(ncKey, newDecodedResponse, reservationId, false);
                } else {
                    invalidateNearCache(ncKey);
                }
            }, getClient().getTaskScheduler());
        }

        return new ClientDelegatingFuture<>(getAsyncInternal(key),
                getSerializationService(), MapGetCodec::decodeResponse);
    }

    @Override
    protected Data removeInternal(Object key) {
        key = toNearCacheKey(key);
        try {
            return super.removeInternal(key);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean removeInternal(Object key, Object value) {
        key = toNearCacheKey(key);
        boolean removed;
        try {
            removed = super.removeInternal(key, value);
        } finally {
            invalidateNearCache(key);
        }
        return removed;
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
        key = toNearCacheKey(key);
        try {
            super.deleteInternal(key);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected InternalCompletableFuture<V> putAsyncInternal(long ttl, TimeUnit timeunit, Long maxIdle, TimeUnit maxIdleUnit,
                                                            Object key, Object value) {
        key = toNearCacheKey(key);
        InternalCompletableFuture<V> future;
        try {
            future = super.putAsyncInternal(ttl, timeunit, maxIdle, maxIdleUnit, key, value);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    protected InternalCompletableFuture<Void> setAsyncInternal(long ttl, TimeUnit timeunit, Long maxIdle, TimeUnit maxIdleUnit,
                                                               Object key, Object value) {
        key = toNearCacheKey(key);
        InternalCompletableFuture<Void> future;
        try {
            future = super.setAsyncInternal(ttl, timeunit, maxIdle, maxIdleUnit, key, value);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    protected InternalCompletableFuture<V> removeAsyncInternal(Object key) {
        key = toNearCacheKey(key);
        InternalCompletableFuture<V> future;
        try {
            future = super.removeAsyncInternal(key);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    protected boolean tryRemoveInternal(long timeout, TimeUnit timeunit, Object key) {
        key = toNearCacheKey(key);
        boolean removed;
        try {
            removed = super.tryRemoveInternal(timeout, timeunit, key);
        } finally {
            invalidateNearCache(key);
        }
        return removed;
    }

    @Override
    protected boolean tryPutInternal(long timeout, TimeUnit timeunit,
                                     Object key, Object value) {
        key = toNearCacheKey(key);
        boolean putInternal;
        try {
            putInternal = super.tryPutInternal(timeout, timeunit, key, value);
        } finally {
            invalidateNearCache(key);
        }
        return putInternal;
    }

    @Override
    protected V putInternal(long ttl, TimeUnit ttlUnit, Long maxIdle, TimeUnit maxIdleUnit, Object key, Object value) {
        key = toNearCacheKey(key);
        V previousValue;
        try {
            previousValue = super.putInternal(ttl, ttlUnit, maxIdle, maxIdleUnit, key, value);
        } finally {
            invalidateNearCache(key);
        }
        return previousValue;
    }

    @Override
    protected void putTransientInternal(long ttl, TimeUnit timeunit, Long maxIdle, TimeUnit maxIdleUnit,
                                        Object key, Object value) {
        key = toNearCacheKey(key);
        try {
            super.putTransientInternal(ttl, timeunit, maxIdle, maxIdleUnit, key, value);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected V putIfAbsentInternal(long ttl, TimeUnit timeunit, Long maxIdle, TimeUnit maxIdleUnit,
                                    Object key, Object value) {
        key = toNearCacheKey(key);
        V previousValue;
        try {
            previousValue = super.putIfAbsentInternal(ttl, timeunit, maxIdle, maxIdleUnit, key, value);
        } finally {
            invalidateNearCache(key);
        }
        return previousValue;
    }

    @Override
    protected boolean replaceIfSameInternal(Object key, Object oldValue, Object newValue) {
        key = toNearCacheKey(key);
        boolean replaceIfSame;
        try {
            replaceIfSame = super.replaceIfSameInternal(key, oldValue, newValue);
        } finally {
            invalidateNearCache(key);
        }
        return replaceIfSame;
    }

    @Override
    protected V replaceInternal(Object key, Object value) {
        key = toNearCacheKey(key);
        V v;
        try {
            v = super.replaceInternal(key, value);
        } finally {
            invalidateNearCache(key);
        }
        return v;
    }

    @Override
    protected void setInternal(long ttl, TimeUnit timeunit, Long maxIdle, TimeUnit maxIdleUnit, Object key, Object value) {
        key = toNearCacheKey(key);
        try {
            super.setInternal(ttl, timeunit, maxIdle, maxIdleUnit, key, value);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean evictInternal(Object key) {
        key = toNearCacheKey(key);
        boolean evicted;
        try {
            evicted = super.evictInternal(key);
        } finally {
            invalidateNearCache(key);
        }
        return evicted;
    }

    @Override
    public void evictAll() {
        try {
            super.evictAll();
        } finally {
            nearCache.clear();
        }
    }

    @Override
    public void loadAll(boolean replaceExistingValues) {
        super.loadAll(replaceExistingValues);
        if (replaceExistingValues) {
            nearCache.clear();
        }
    }

    @Override
    protected void loadAllInternal(boolean replaceExistingValues, Collection<?> keysParameter) {
        Collection<?> keys = serializeKeys ? objectToDataCollection(keysParameter, getSerializationService()) : keysParameter;

        try {
            super.loadAllInternal(replaceExistingValues, keys);
        } finally {
            for (Object key : keys) {
                invalidateNearCache(key);
            }
        }
    }

    @Override
    protected void getAllInternal(Set<K> keys, Map<Integer, List<Data>> partitionToKeyData, List<Object> resultingKeyValuePairs) {
        Map<Object, Data> keyMap = createHashMap(keys.size());
        if (serializeKeys) {
            fillPartitionToKeyData(keys, partitionToKeyData, keyMap, null);
        }
        Collection<?> ncKeys = serializeKeys ? keyMap.values() : new LinkedList<>(keys);

        populateResultFromNearCache(ncKeys, resultingKeyValuePairs);
        if (ncKeys.isEmpty()) {
            return;
        }

        Map<Data, Object> reverseKeyMap = null;
        if (!serializeKeys) {
            reverseKeyMap = createHashMap(ncKeys.size());
            fillPartitionToKeyData(keys, partitionToKeyData, keyMap, reverseKeyMap);
        }

        Map<Object, Long> reservations = getNearCacheReservations(ncKeys, keyMap);
        try {
            int currentSize = resultingKeyValuePairs.size();
            super.getAllInternal(keys, partitionToKeyData, resultingKeyValuePairs);
            populateResultFromRemote(currentSize, resultingKeyValuePairs, reservations, reverseKeyMap);
        } finally {
            releaseRemainingReservedKeys(reservations);
        }
    }

    private void populateResultFromNearCache(Collection<?> keys, List<Object> resultingKeyValuePairs) {
        Iterator<?> iterator = keys.iterator();
        while (iterator.hasNext()) {
            Object key = iterator.next();
            Object cached = getCachedValue(key, true);
            if (cached != null && cached != NOT_CACHED) {
                resultingKeyValuePairs.add(key);
                resultingKeyValuePairs.add(cached);
                iterator.remove();
            }
        }
    }

    private Map<Object, Long> getNearCacheReservations(Collection<?> nearCacheKeys, Map<Object, Data> keyMap) {
        Map<Object, Long> reservations = createHashMap(nearCacheKeys.size());
        for (Object key : nearCacheKeys) {
            Data keyData = serializeKeys ? (Data) key : keyMap.get(key);
            long reservationId = nearCache.tryReserveForUpdate(key, keyData, READ_UPDATE);
            if (reservationId != NOT_RESERVED) {
                reservations.put(key, reservationId);
            }
        }
        return reservations;
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

    private void releaseRemainingReservedKeys(Map<Object, Long> reservedKeys) {
        for (Entry<Object, Long> entry : reservedKeys.entrySet()) {
            Object key = serializeKeys ? toData(entry.getKey()) : entry.getKey();
            invalidateNearCache(key);
        }
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        LocalMapStatsImpl localMapStats = (LocalMapStatsImpl) super.getLocalMapStats();
        localMapStats.setNearCacheStats(nearCache.getNearCacheStats());
        return localMapStats;
    }

    @Nonnull
    @Override
    protected <R> InternalCompletableFuture<Map<K, R>> submitToKeysInternal(@Nonnull Set<K> objectKeys,
                                                                            @Nonnull Collection<Data> dataKeys,
                                                                            @Nonnull EntryProcessor<K, V, R> entryProcessor) {
        try {
            return super.submitToKeysInternal(objectKeys, dataKeys, entryProcessor);
        } finally {
            Collection<?> nearCacheKeys = serializeKeys ? dataKeys : objectKeys;
            nearCacheKeys.forEach(this::invalidateNearCache);
        }
    }

    @Override
    public <R> R executeOnKeyInternal(Object key,
                                      EntryProcessor<K, V, R> entryProcessor) {
        key = toNearCacheKey(key);
        R response;
        try {
            response = super.executeOnKeyInternal(key, entryProcessor);
        } finally {
            invalidateNearCache(key);
        }
        return response;
    }

    @Override
    public <R> InternalCompletableFuture<R> submitToKeyInternal(Object key,
                                                                EntryProcessor<K, V, R> entryProcessor) {
        key = toNearCacheKey(key);
        InternalCompletableFuture future;
        try {
            future = super.submitToKeyInternal(key, entryProcessor);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    protected <R> Map<K, R> prepareResult(Collection<Entry<Data, Data>> entrySet) {
        if (CollectionUtil.isEmpty(entrySet)) {
            return emptyMap();
        }
        Map<K, R> result = createHashMap(entrySet.size());
        for (Entry<Data, Data> entry : entrySet) {
            Data dataKey = entry.getKey();
            K key = toObject(dataKey);
            R value = toObject(entry.getValue());

            invalidateNearCache(serializeKeys ? dataKey : key);
            result.put(key, value);
        }
        return result;
    }

    @Override
    protected void finalizePutAll(Map<? extends K, ? extends V> map, Map<Integer, List<Map.Entry<Data, Data>>> entryMap) {
        if (serializeKeys) {
            for (List<Entry<Data, Data>> entries : entryMap.values()) {
                for (Entry<Data, Data> entry : entries) {
                    invalidateNearCache(entry.getKey());
                }
            }
        } else {
            for (K key : map.keySet()) {
                invalidateNearCache(key);
            }
        }
    }

    @Override
    public void clear() {
        nearCache.clear();
        super.clear();
    }

    @Override
    protected void postDestroy() {
        try {
            destroyNearCache();
        } finally {
            super.postDestroy();
        }
    }

    @Override
    protected void onShutdown() {
        try {
            destroyNearCache();
        } finally {
            super.onShutdown();
        }
    }

    private void destroyNearCache() {
        removeNearCacheInvalidationListener();
        getContext().getNearCacheManager(getServiceName()).destroyNearCache(name);
    }

    private Object toNearCacheKey(Object key) {
        return serializeKeys ? toData(key) : key;
    }

    private Object tryPublishReserved(Object key, Object value, long reservationId) {
        assert value != NOT_CACHED;

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
        return deserializeValue ? toObject(value) : value;
    }

    public NearCache<Object, Object> getNearCache() {
        return nearCache;
    }

    private void invalidateNearCache(Object key) {
        nearCache.invalidate(key);
    }

    private void registerInvalidationListener() {
        try {
            invalidationListenerId = addNearCacheInvalidationListener(new NearCacheInvalidationEventHandler());
        } catch (Exception e) {
            ILogger logger = getContext().getLoggingService().getLogger(getClass());
            logger.severe("-----------------\nNear Cache is not initialized!\n-----------------", e);
        }
    }

    public UUID addNearCacheInvalidationListener(EventHandler handler) {
        return registerListener(createNearCacheInvalidationListenerCodec(), handler);
    }

    @SuppressWarnings("checkstyle:anoninnerlength")
    private ListenerMessageCodec createNearCacheInvalidationListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MapAddNearCacheInvalidationListenerCodec.encodeRequest(name, INVALIDATION.getType(), localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return MapAddNearCacheInvalidationListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    private void removeNearCacheInvalidationListener() {
        UUID invalidationListenerId = this.invalidationListenerId;
        if (invalidationListenerId == null) {
            return;
        }

        getContext().getRepairingTask(getServiceName()).deregisterHandler(name);
        deregisterListener(invalidationListenerId);
    }

    /**
     * This listener listens server side invalidation events.
     */
    private final class NearCacheInvalidationEventHandler
            extends MapAddNearCacheInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private volatile RepairingHandler repairingHandler;

        @Override
        public void beforeListenerRegister(Connection connection) {
            RepairingTask repairingTask = getContext().getRepairingTask(getServiceName());
            repairingHandler = repairingTask.registerAndGetHandler(name, nearCache);
        }

        @Override
        public void handleIMapInvalidationEvent(Data key, UUID sourceUuid,
                                                UUID partitionUuid, long sequence) {
            repairingHandler.handle(key, sourceUuid, partitionUuid, sequence);
        }


        @Override
        public void handleIMapBatchInvalidationEvent(Collection<Data> keys,
                                                     Collection<UUID> sourceUuids,
                                                     Collection<UUID> partitionUuids,
                                                     Collection<Long> sequences) {
            repairingHandler.handle(keys, sourceUuids, partitionUuids, sequences);
        }
    }
}
