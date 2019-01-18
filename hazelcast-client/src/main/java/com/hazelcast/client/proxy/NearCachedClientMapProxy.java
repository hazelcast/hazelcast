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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveEntryListenerCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.adapter.IMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.instance.BuildInfo.calculateVersion;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;

/**
 * A Client-side {@code IMap} implementation which is fronted by a Near Cache.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class NearCachedClientMapProxy<K, V> extends ClientMapProxy<K, V> {

    // eventually consistent Near Cache can only be used with server versions >= 3.8
    private final int minConsistentNearCacheSupportingServerVersion = calculateVersion("3.8");

    private ILogger logger;
    private boolean serializeKeys;
    private NearCache<Object, Object> nearCache;

    private volatile String invalidationListenerId;

    public NearCachedClientMapProxy(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        logger = getContext().getLoggingService().getLogger(getClass());

        NearCacheConfig nearCacheConfig = getContext().getClientConfig().getNearCacheConfig(name);
        serializeKeys = nearCacheConfig.isSerializeKeys();

        NearCacheManager nearCacheManager = getContext().getNearCacheManager();
        IMapDataStructureAdapter<K, V> adapter = new IMapDataStructureAdapter<K, V>(this);
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
    public ICompletableFuture<V> getAsync(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Object ncKey = toNearCacheKey(key);
        Object value = getCachedValue(ncKey, false);
        if (value != NOT_CACHED) {
            ExecutorService executor = getContext().getExecutionService().getUserExecutor();
            return new CompletedFuture<V>(getSerializationService(), value, executor);
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
            invocationFuture.andThen(new ExecutionCallback<ClientMessage>() {
                @Override
                public void onResponse(ClientMessage response) {
                    Object newDecodedResponse = GET_ASYNC_RESPONSE_DECODER.decodeClientMessage(response);
                    nearCache.tryPublishReserved(ncKey, newDecodedResponse, reservationId, false);
                }

                @Override
                public void onFailure(Throwable t) {
                    invalidateNearCache(ncKey);
                }
            }, getClient().getClientExecutionService());
        }

        return new ClientDelegatingFuture<V>(getAsyncInternal(key),
                getSerializationService(), GET_ASYNC_RESPONSE_DECODER);
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
    protected ICompletableFuture<V> putAsyncInternal(long ttl, TimeUnit timeunit, Long maxIdle, TimeUnit maxIdleUnit,
                                                     Object key, Object value) {
        key = toNearCacheKey(key);
        ICompletableFuture<V> future;
        try {
            future = super.putAsyncInternal(ttl, timeunit, maxIdle, maxIdleUnit, key, value);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    protected ICompletableFuture<Void> setAsyncInternal(long ttl, TimeUnit timeunit, Long maxIdle, TimeUnit maxIdleUnit,
                                                        Object key, Object value) {
        key = toNearCacheKey(key);
        ICompletableFuture<Void> future;
        try {
            future = super.setAsyncInternal(ttl, timeunit, maxIdle, maxIdleUnit, key, value);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    protected ICompletableFuture<V> removeAsyncInternal(Object key) {
        key = toNearCacheKey(key);
        ICompletableFuture<V> future;
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
        Collection<?> ncKeys = serializeKeys ? keyMap.values() : new LinkedList<K>(keys);

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
            long reservationId = nearCache.tryReserveForUpdate(key, keyData);
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

    @Override
    public Object executeOnKeyInternal(Object key, EntryProcessor entryProcessor) {
        key = toNearCacheKey(key);
        Object response;
        try {
            response = super.executeOnKeyInternal(key, entryProcessor);
        } finally {
            invalidateNearCache(key);
        }
        return response;
    }

    @Override
    public ICompletableFuture submitToKeyInternal(Object key, EntryProcessor entryProcessor) {
        key = toNearCacheKey(key);
        ICompletableFuture future;
        try {
            future = super.submitToKeyInternal(key, entryProcessor);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    public void submitToKeyInternal(Object key, EntryProcessor entryProcessor, ExecutionCallback callback) {
        key = toNearCacheKey(key);
        try {
            super.submitToKeyInternal(key, entryProcessor, callback);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected Map<K, Object> prepareResult(Collection<Entry<Data, Data>> entrySet) {
        if (CollectionUtil.isEmpty(entrySet)) {
            return emptyMap();
        }
        Map<K, Object> result = createHashMap(entrySet.size());
        for (Entry<Data, Data> entry : entrySet) {
            Data dataKey = entry.getKey();
            K key = toObject(dataKey);
            V value = toObject(entry.getValue());

            invalidateNearCache(serializeKeys ? dataKey : key);
            result.put(key, value);
        }
        return result;
    }

    @Override
    protected void putAllInternal(Map<? extends K, ? extends V> map, Map<Integer, List<Map.Entry<Data, Data>>> entryMap) {
        try {
            super.putAllInternal(map, entryMap);
        } finally {
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
    }

    @Override
    public void clear() {
        nearCache.clear();
        super.clear();
    }

    @Override
    protected void postDestroy() {
        try {
            removeNearCacheInvalidationListener();
            getContext().getNearCacheManager().destroyNearCache(name);
        } finally {
            super.postDestroy();
        }
    }

    @Override
    protected void onShutdown() {
        removeNearCacheInvalidationListener();
        getContext().getNearCacheManager().destroyNearCache(name);

        super.onShutdown();
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

    public String addNearCacheInvalidationListener(EventHandler handler) {
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
                if (supportsRepairableNearCache()) {
                    // this is for servers >= 3.8
                    return MapAddNearCacheInvalidationListenerCodec.encodeRequest(name, INVALIDATION.getType(), localOnly);
                }

                // this is for servers < 3.8
                return MapAddNearCacheEntryListenerCodec.encodeRequest(name, INVALIDATION.getType(), localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                if (supportsRepairableNearCache()) {
                    // this is for servers >= 3.8
                    return MapAddNearCacheInvalidationListenerCodec.decodeResponse(clientMessage).response;
                }

                // this is for servers < 3.8
                return MapAddNearCacheEntryListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    private void removeNearCacheInvalidationListener() {
        String invalidationListenerId = this.invalidationListenerId;
        if (invalidationListenerId == null) {
            return;
        }

        getContext().getRepairingTask(getServiceName()).deregisterHandler(name);
        deregisterListener(invalidationListenerId);
    }

    private boolean supportsRepairableNearCache() {
        return getConnectedServerVersion() >= minConsistentNearCacheSupportingServerVersion;
    }

    /**
     * Eventual consistency for Near Cache can be used with server versions >= 3.8
     * For repairing functionality please see {@link RepairingHandler}
     * handleCacheInvalidationEventV14 and handleCacheBatchInvalidationEventV14
     *
     * If server version is < 3.8 and client version is >= 3.8, eventual consistency is not supported
     * Following methods handle the old behaviour:
     * handleCacheBatchInvalidationEventV10 and handleCacheInvalidationEventV10
     */
    private final class NearCacheInvalidationEventHandler
            extends MapAddNearCacheInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private volatile RepairingHandler repairingHandler;
        private volatile boolean supportsRepairableNearCache;

        @Override
        public void beforeListenerRegister() {
            supportsRepairableNearCache = supportsRepairableNearCache();

            if (supportsRepairableNearCache) {
                RepairingTask repairingTask = getContext().getRepairingTask(getServiceName());
                repairingHandler = repairingTask.registerAndGetHandler(name, nearCache);
            } else {
                nearCache.clear();
                RepairingTask repairingTask = getContext().getRepairingTask(getServiceName());
                repairingTask.deregisterHandler(name);
                logger.warning(format("Near Cache for '%s' map is started in legacy mode", name));
            }
        }

        @Override
        public void onListenerRegister() {
            if (!supportsRepairableNearCache) {
                nearCache.clear();
            }
        }

        @Override
        public void handleIMapInvalidationEventV10(Data key) {
            if (key != null) {
                nearCache.invalidate(serializeKeys ? key : toObject(key));
            } else {
                nearCache.clear();
            }
        }

        @Override
        public void handleIMapBatchInvalidationEventV10(Collection<Data> keys) {
            for (Data key : keys) {
                nearCache.invalidate(serializeKeys ? key : toObject(key));
            }
        }

        @Override
        public void handleIMapInvalidationEventV14(Data key, String sourceUuid,
                                                   UUID partitionUuid, long sequence) {
            repairingHandler.handle(key, sourceUuid, partitionUuid, sequence);
        }


        @Override
        public void handleIMapBatchInvalidationEventV14(Collection<Data> keys,
                                                        Collection<String> sourceUuids,
                                                        Collection<UUID> partitionUuids,
                                                        Collection<Long> sequences) {
            repairingHandler.handle(keys, sourceUuids, partitionUuids, sequences);
        }
    }
}
