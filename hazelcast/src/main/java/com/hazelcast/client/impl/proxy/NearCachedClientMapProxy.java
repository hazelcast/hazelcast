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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveEntryListenerCodec;
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.internal.serialization.Data;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;

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
        IMapDataStructureAdapter<K, V> adapter = new IMapDataStructureAdapter<>(this);
        nearCache = nearCacheManager.getOrCreateNearCache(name, nearCacheConfig, adapter);

        if (nearCacheConfig.isInvalidateOnChange()) {
            registerInvalidationListener();
        }
    }

    @Override
    protected boolean containsKeyInternal(Object key) {
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
            long reservationId = nearCache.tryReserveForUpdate(key, keyData);
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
            ExecutorService executor = getContext().getExecutionService().getUserExecutor();
            return newCompletedFuture(value, getSerializationService(), executor);
        }

        Data keyData = toData(ncKey);
        final long reservationId = nearCache.tryReserveForUpdate(ncKey, keyData);
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
                    Object newDecodedResponse = MapGetCodec.decodeResponse(response).response;
                    nearCache.tryPublishReserved(ncKey, newDecodedResponse, reservationId, false);
                } else {
                    invalidateNearCache(ncKey);
                }
            }, getClient().getClientExecutionService());
        }

        return new ClientDelegatingFuture<>(getAsyncInternal(key),
                getSerializationService(), clientMessage -> MapGetCodec.decodeResponse(clientMessage).response);
    }

    @Override
    protected MapRemoveCodec.ResponseParameters removeInternal(Object key) {
        key = toNearCacheKey(key);
        MapRemoveCodec.ResponseParameters responseParameters;
        try {
            responseParameters = super.removeInternal(key);
        } finally {
            invalidateNearCache(key);
        }
        return responseParameters;
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
    protected void getAllInternal(Set<K> keys, Map<Integer, List<Data>> partitionToKeyData, Map<K, V> result) {
        Map<Object, Data> keyMap = createHashMap(keys.size());
        if (serializeKeys) {
            fillPartitionToKeyData(keys, partitionToKeyData, keyMap);
        }
        Collection<?> ncKeys = serializeKeys ? keyMap.values() : new LinkedList<>(keys);

        Map<K, V> nearCacheResult = createHashMap(ncKeys.size());
        populateResultFromNearCache(ncKeys, nearCacheResult);
        if (ncKeys.isEmpty()) {
            result.putAll(nearCacheResult);
            return;
        }

        if (!serializeKeys) {
            fillPartitionToKeyData(keys, partitionToKeyData, keyMap);
        }

        Map<Object, Long> reservations = getNearCacheReservations(ncKeys, keyMap);
        try {
            super.getAllInternal(keys, partitionToKeyData, result);
            populateResultFromRemote(result, reservations, keyMap);
            result.putAll(nearCacheResult);
        } finally {
            releaseRemainingReservedKeys(reservations);
        }
    }

    private void populateResultFromNearCache(Collection<?> keys, Map<K, V> result) {
        Iterator<?> iterator = keys.iterator();
        while (iterator.hasNext()) {
            Object key = iterator.next();
            Object cached = getCachedValue(key, true);
            if (cached != null && cached != NOT_CACHED) {
                result.put(toObject(key), (V) cached);
                iterator.remove();
            }
        }
    }

    private Map<Object, Long> getNearCacheReservations(Collection<?> nearCacheKeys, Map<Object, Data> keyMap) {
        Map<Object, Long> reservations = createHashMap(nearCacheKeys.size());
        for (Object key : nearCacheKeys) {
            Data keyData = serializeKeys ? (Data) key : keyMap.get(key);
            long reservationId = nearCache.tryReserveForUpdate(key, keyData);
            if (reservationId != NOT_RESERVED) {
                reservations.put(key, reservationId);
            }
        }
        return reservations;
    }

    private void populateResultFromRemote(Map<K, V> result, Map<Object, Long> reservations, Map<Object, Data> keyMap) {
        for (Map.Entry<K, V> entry : result.entrySet()) {
            K key = entry.getKey();
            Object ncKey = serializeKeys ? keyMap.get(key) : key;

            Long reservationId = reservations.get(ncKey);
            if (reservationId != null) {
                Object cachedValue = tryPublishReserved(ncKey, entry.getValue(), reservationId);
                result.put(key, (V) cachedValue);
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
    protected <R> BiConsumer<Data, Data> createResponseConsumer(Map<K, R> result) {
        return (key, value) -> {
            K deserializedKey = toObject(key);
            invalidateNearCache(serializeKeys ? key : deserializedKey);
            result.put(deserializedKey, toObject(value));
        };
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

    public UUID addNearCacheInvalidationListener(EventHandler handler) {
        return registerListener(createNearCacheEntryListenerCodec(), handler);
    }

    private void registerInvalidationListener() {
        try {
            invalidationListenerId = addNearCacheInvalidationListener(new NearCacheInvalidationEventHandler());
        } catch (Exception e) {
            ILogger logger = getContext().getLoggingService().getLogger(getClass());
            logger.severe("-----------------\nNear Cache is not initialized!\n-----------------", e);
        }
    }

    @SuppressWarnings("checkstyle:anoninnerlength")
    private ListenerMessageCodec createNearCacheEntryListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MapAddNearCacheInvalidationListenerCodec.encodeRequest(name, INVALIDATION.getType(), localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return MapAddNearCacheInvalidationListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
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
