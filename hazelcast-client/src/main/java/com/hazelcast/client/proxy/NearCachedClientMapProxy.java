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

package com.hazelcast.client.proxy;

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MapGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveEntryListenerCodec;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.EventHandler;
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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.instance.BuildInfo.UNKNOWN_HAZELCAST_VERSION;
import static com.hazelcast.instance.BuildInfo.calculateVersion;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.createHashMap;
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
            return true;
        }
        return super.containsKeyInternal(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V getInternal(Object key) {
        key = serializeKeys ? toData(key) : key;
        V value = (V) getCachedValue(key, true);
        if (value != NOT_CACHED) {
            return value;
        }

        try {
            long reservationId = nearCache.tryReserveForUpdate(key);
            Data keyData = toData(key);
            value = (V) super.getInternal(keyData);
            if (reservationId != NOT_RESERVED) {
                value = (V) tryPublishReserved(key, keyData, value, reservationId);
            }
            return value;
        } catch (Throwable throwable) {
            invalidateNearCache(key);
            throw rethrow(throwable);
        }
    }

    @Override
    public ICompletableFuture<V> getAsyncInternal(Object keyParameter) {
        final Object key = serializeKeys ? toData(keyParameter) : keyParameter;
        Object value = getCachedValue(key, false);
        if (value != NOT_CACHED) {
            ExecutorService executor = getContext().getExecutionService().getUserExecutor();
            return new CompletedFuture<V>(getSerializationService(), value, executor);
        }

        final long reservationId = nearCache.tryReserveForUpdate(key);
        final Data keyData = toData(key);
        ICompletableFuture<V> future;
        try {
            future = super.getAsyncInternal(keyData);
        } catch (Throwable t) {
            invalidateNearCache(key);
            throw rethrow(t);
        }

        if (reservationId != NOT_RESERVED) {
            ((ClientDelegatingFuture) future).andThenInternal(new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object value) {
                    nearCache.tryPublishReserved(key, keyData, value, reservationId, false);
                }

                @Override
                public void onFailure(Throwable t) {
                    invalidateNearCache(key);
                }
            }, false);
        }

        return future;
    }

    @Override
    protected MapRemoveCodec.ResponseParameters removeInternal(Object key) {
        key = serializeKeys ? toData(key) : key;
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
        key = serializeKeys ? toData(key) : key;
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
        key = serializeKeys ? toData(key) : key;
        try {
            super.deleteInternal(key);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected ICompletableFuture<V> putAsyncInternal(long ttl, TimeUnit timeunit, Object key, Object value) {
        key = serializeKeys ? toData(key) : key;
        ICompletableFuture<V> future;
        try {
            future = super.putAsyncInternal(ttl, timeunit, key, value);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    protected ICompletableFuture<Void> setAsyncInternal(long ttl, TimeUnit timeunit, Object key, Object value) {
        key = serializeKeys ? toData(key) : key;
        ICompletableFuture<Void> future;
        try {
            future = super.setAsyncInternal(ttl, timeunit, key, value);
        } finally {
            invalidateNearCache(key);
        }
        return future;
    }

    @Override
    protected ICompletableFuture<V> removeAsyncInternal(Object key) {
        key = serializeKeys ? toData(key) : key;
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
        key = serializeKeys ? toData(key) : key;
        boolean removed;
        try {
            removed = super.tryRemoveInternal(timeout, timeunit, key);
        } finally {
            invalidateNearCache(key);
        }
        return removed;
    }

    @Override
    protected boolean tryPutInternal(long timeout, TimeUnit timeunit, Object key, Object value) {
        key = serializeKeys ? toData(key) : key;
        boolean putInternal;
        try {
            putInternal = super.tryPutInternal(timeout, timeunit, key, value);
        } finally {
            invalidateNearCache(key);
        }
        return putInternal;
    }

    @Override
    protected V putInternal(long ttl, TimeUnit timeunit, Object key, Object value) {
        key = serializeKeys ? toData(key) : key;
        V previousValue;
        try {
            previousValue = super.putInternal(ttl, timeunit, key, value);
        } finally {
            invalidateNearCache(key);
        }
        return previousValue;
    }

    @Override
    protected void putTransientInternal(long ttl, TimeUnit timeunit, Object key, Object value) {
        key = serializeKeys ? toData(key) : key;
        try {
            super.putTransientInternal(ttl, timeunit, key, value);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected V putIfAbsentInternal(long ttl, TimeUnit timeunit, Object key, Object value) {
        key = serializeKeys ? toData(key) : key;
        V previousValue;
        try {
            previousValue = super.putIfAbsentInternal(ttl, timeunit, key, value);
        } finally {
            invalidateNearCache(key);
        }
        return previousValue;
    }

    @Override
    protected boolean replaceIfSameInternal(Object key, Object oldValue, Object newValue) {
        key = serializeKeys ? toData(key) : key;
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
        key = serializeKeys ? toData(key) : key;
        V v;
        try {
            v = super.replaceInternal(key, value);
        } finally {
            invalidateNearCache(key);
        }
        return v;
    }

    @Override
    protected void setInternal(long ttl, TimeUnit timeunit, Object key, Object value) {
        key = serializeKeys ? toData(key) : key;
        try {
            super.setInternal(ttl, timeunit, key, value);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    protected boolean evictInternal(Object key) {
        key = serializeKeys ? toData(key) : key;
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
    @SuppressWarnings("unchecked")
    protected List<MapGetAllCodec.ResponseParameters> getAllInternal(Set<K> keys, Map<Integer, List<Data>> partitionToKeyData,
                                                                     Map<K, V> result) {
        Map<Object, Long> reservations = new HashMap<Object, Long>();
        try {
            if (serializeKeys) {
                fillPartitionToKeyData(keys, partitionToKeyData);
                for (List<Data> keyList : partitionToKeyData.values()) {
                    populateResultFromNearCache(result, reservations, keyList.iterator());
                }
            } else {
                populateResultFromNearCache(result, reservations, keys.iterator());
                fillPartitionToKeyData(keys, partitionToKeyData);
            }
            List<MapGetAllCodec.ResponseParameters> responses = super.getAllInternal(keys, partitionToKeyData, result);
            for (MapGetAllCodec.ResponseParameters resultParameters : responses) {
                for (Entry<Data, Data> entry : resultParameters.response) {
                    Data dataKey = entry.getKey();
                    Data dataValue = entry.getValue();
                    Object key = serializeKeys ? dataKey : toObject(dataKey);
                    Long reservationId = reservations.get(key);
                    if (reservationId != null) {
                        Object cachedValue = tryPublishReserved(key, dataKey, dataValue, reservationId);
                        result.put((K) (serializeKeys ? toObject(key) : key), (V) toObject(cachedValue));
                        reservations.remove(key);
                    }
                }
            }
            return responses;
        } finally {
            releaseRemainingReservedKeys(reservations);
        }
    }

    @SuppressWarnings("unchecked")
    private void populateResultFromNearCache(Map<K, V> result, Map<Object, Long> reservations, Iterator<?> keyIterator) {
        while (keyIterator.hasNext()) {
            Object key = keyIterator.next();
            V value = (V) getCachedValue(key, true);
            if (value == null || value == NOT_CACHED) {
                long reservationId = nearCache.tryReserveForUpdate(key);
                if (reservationId != NOT_RESERVED) {
                    reservations.put(key, reservationId);
                }
                continue;
            }
            result.put((K) (serializeKeys ? toObject(key) : key), value);
            keyIterator.remove();
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
        key = serializeKeys ? toData(key) : key;
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
        key = serializeKeys ? toData(key) : key;
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
        key = serializeKeys ? toData(key) : key;
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
    protected void onDestroy() {
        removeNearCacheInvalidationListener();
        getContext().getNearCacheManager().destroyNearCache(name);

        super.onDestroy();
    }

    @Override
    protected void onShutdown() {
        removeNearCacheInvalidationListener();
        getContext().getNearCacheManager().destroyNearCache(name);

        super.onShutdown();
    }

    private Object tryPublishReserved(Object key, Data keyData, Object value, long reservationId) {
        assert value != NOT_CACHED;

        Object cachedValue = nearCache.tryPublishReserved(key, keyData, value, reservationId, true);
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
        nearCache.remove(key);
    }

    public String addNearCacheInvalidationListener(EventHandler handler) {
        return registerListener(createNearCacheEntryListenerCodec(), handler);
    }

    private void registerInvalidationListener() {
        try {
            invalidationListenerId = addNearCacheInvalidationListener(new ConnectedServerVersionAwareNearCacheEventHandler());
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

        getContext().getRepairingTask(SERVICE_NAME).deregisterHandler(name);
        deregisterListener(invalidationListenerId);
    }

    /**
     * Deals with client compatibility.
     * <p>
     * Eventual consistency for Near Cache can be used with server versions >= 3.8,
     * other connected server versions must use {@link Pre38NearCacheEventHandler}
     */
    private final class ConnectedServerVersionAwareNearCacheEventHandler implements EventHandler<ClientMessage> {

        private final RepairableNearCacheEventHandler repairingEventHandler = new RepairableNearCacheEventHandler();
        private final Pre38NearCacheEventHandler pre38EventHandler = new Pre38NearCacheEventHandler();

        private volatile boolean supportsRepairableNearCache;

        @Override
        public void beforeListenerRegister() {
            repairingEventHandler.beforeListenerRegister();
            pre38EventHandler.beforeListenerRegister();
        }

        @Override
        public void onListenerRegister() {
            supportsRepairableNearCache = supportsRepairableNearCache();

            if (supportsRepairableNearCache) {
                repairingEventHandler.onListenerRegister();
            } else {
                pre38EventHandler.onListenerRegister();
            }
        }

        @Override
        public void handle(ClientMessage clientMessage) {
            if (supportsRepairableNearCache) {
                repairingEventHandler.handle(clientMessage);
            } else {
                pre38EventHandler.handle(clientMessage);
            }
        }
    }

    /**
     * This event handler can only be used with server versions >= 3.8 and supports Near Cache eventual consistency improvements.
     * For repairing functionality please see {@link RepairingHandler}.
     */
    private final class RepairableNearCacheEventHandler
            extends MapAddNearCacheInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private volatile RepairingHandler repairingHandler;

        @Override
        public void beforeListenerRegister() {
            nearCache.clear();
            getRepairingTask().deregisterHandler(name);
        }

        @Override
        public void onListenerRegister() {
            nearCache.clear();
            repairingHandler = getRepairingTask().registerAndGetHandler(name, nearCache);
        }

        @Override
        public void handle(Data key, String sourceUuid, UUID partitionUuid, long sequence) {
            repairingHandler.handle(key, sourceUuid, partitionUuid, sequence);
        }

        @Override
        public void handle(Collection<Data> keys, Collection<String> sourceUuids,
                           Collection<UUID> partitionUuids, Collection<Long> sequences) {
            repairingHandler.handle(keys, sourceUuids, partitionUuids, sequences);
        }

        private RepairingTask getRepairingTask() {
            return getContext().getRepairingTask(SERVICE_NAME);
        }
    }

    /**
     * This event handler is here to be used with server versions < 3.8.
     * <p>
     * If server version is < 3.8 and client version is >= 3.8, this event handler must be used to
     * listen Near Cache invalidations. Because new improvements for Near Cache eventual consistency
     * cannot work with server versions < 3.8.
     */
    private final class Pre38NearCacheEventHandler
            extends MapAddNearCacheEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        @Override
        public void beforeListenerRegister() {
            nearCache.clear();
        }

        @Override
        public void onListenerRegister() {
            nearCache.clear();
        }

        @Override
        public void handle(Data key, String sourceUuid, UUID partitionUuid, long sequence) {
            // null key means that the Near Cache has to remove all entries in it
            // (see Pre38MapAddNearCacheEntryListenerMessageTask)
            if (key == null) {
                nearCache.clear();
            } else {
                nearCache.remove(serializeKeys ? key : toObject(key));
            }
        }

        @Override
        public void handle(Collection<Data> keys, Collection<String> sourceUuids,
                           Collection<UUID> partitionUuids, Collection<Long> sequences) {
            for (Data key : keys) {
                nearCache.remove(serializeKeys ? key : toObject(key));
            }
        }
    }

    private int getConnectedServerVersion() {
        ClientClusterService clusterService = getContext().getClusterService();
        Address ownerConnectionAddress = clusterService.getOwnerConnectionAddress();

        HazelcastClientInstanceImpl client = getClient();
        ClientConnectionManager connectionManager = client.getConnectionManager();
        Connection connection = connectionManager.getConnection(ownerConnectionAddress);
        if (connection == null) {
            logger.warning(format("No owner connection is available, near cached cache %s will be started in legacy mode", name));
            return UNKNOWN_HAZELCAST_VERSION;
        }

        return ((ClientConnection) connection).getConnectedServerVersion();
    }

    private boolean supportsRepairableNearCache() {
        return getConnectedServerVersion() >= minConsistentNearCacheSupportingServerVersion;
    }
}
