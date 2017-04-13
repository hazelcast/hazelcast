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
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.MapUtil;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import static com.hazelcast.util.ExceptionUtil.rethrow;
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

    private NearCache<Object, Object> nearCache;
    private ILogger logger;

    private volatile String invalidationListenerId;

    public NearCachedClientMapProxy(String serviceName, String name) {
        super(serviceName, name);
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        ClientContext context = getContext();
        logger = context.getLoggingService().getLogger(getClass());
        NearCacheConfig nearCacheConfig = context.getClientConfig().getNearCacheConfig(name);
        NearCacheManager nearCacheManager = context.getNearCacheManager();
        IMapDataStructureAdapter<K, V> adapter = new IMapDataStructureAdapter<K, V>(this);
        nearCache = nearCacheManager.getOrCreateNearCache(name, nearCacheConfig, adapter);

        if (nearCacheConfig.isInvalidateOnChange()) {
            addNearCacheInvalidationListener(new ConnectedServerVersionAwareNearCacheEventHandler());
        }
    }

    @Override
    protected boolean containsKeyInternal(Data keyData) {
        Object cached = getCachedValue(keyData, false);
        if (cached != NOT_CACHED) {
            return true;
        }
        return super.containsKeyInternal(keyData);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V getInternal(Data key) {
        V value = (V) getCachedValue(key, true);
        if (value != NOT_CACHED) {
            return value;
        }

        try {
            long reservationId = nearCache.tryReserveForUpdate(key);
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
    public ICompletableFuture<V> getAsyncInternal(final Data key) {
        Object value = getCachedValue(key, false);
        if (value != NOT_CACHED) {
            ExecutorService executor = getContext().getExecutionService().getUserExecutor();
            return new CompletedFuture<V>(getContext().getSerializationService(), value, executor);
        }

        final long reservationId = nearCache.tryReserveForUpdate(key);
        ICompletableFuture<V> future;
        try {
            future = super.getAsyncInternal(key);
        } catch (Throwable t) {
            invalidateNearCache(key);
            throw rethrow(t);
        }

        if (reservationId != NOT_RESERVED) {
            ((ClientDelegatingFuture) future).andThenInternal(new ExecutionCallback<Data>() {
                @Override
                public void onResponse(Data value) {
                    nearCache.tryPublishReserved(key, value, reservationId, false);
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
    protected MapRemoveCodec.ResponseParameters removeInternal(Data keyData) {
        MapRemoveCodec.ResponseParameters responseParameters;
        try {
            responseParameters = super.removeInternal(keyData);
        } finally {
            invalidateNearCache(keyData);
        }
        return responseParameters;
    }

    @Override
    protected boolean removeInternal(Data keyData, Data valueData) {
        boolean removed;
        try {
            removed = super.removeInternal(keyData, valueData);
        } finally {
            invalidateNearCache(keyData);
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
    protected void deleteInternal(Data keyData) {
        try {
            super.deleteInternal(keyData);
        } finally {
            invalidateNearCache(keyData);
        }
    }

    @Override
    protected ICompletableFuture<V> putAsyncInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        ICompletableFuture<V> future;
        try {
            future = super.putAsyncInternal(ttl, timeunit, keyData, valueData);
        } finally {
            invalidateNearCache(keyData);
        }
        return future;
    }

    @Override
    protected ICompletableFuture<Void> setAsyncInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        ICompletableFuture<Void> future;
        try {
            future = super.setAsyncInternal(ttl, timeunit, keyData, valueData);
        } finally {
            invalidateNearCache(keyData);
        }
        return future;
    }

    @Override
    protected ICompletableFuture<V> removeAsyncInternal(Data keyData) {
        ICompletableFuture<V> future;
        try {
            future = super.removeAsyncInternal(keyData);
        } finally {
            invalidateNearCache(keyData);
        }
        return future;
    }

    @Override
    protected boolean tryRemoveInternal(long timeout, TimeUnit timeunit, Data keyData) {
        boolean removed;
        try {
            removed = super.tryRemoveInternal(timeout, timeunit, keyData);
        } finally {
            invalidateNearCache(keyData);
        }
        return removed;
    }

    @Override
    protected boolean tryPutInternal(long timeout, TimeUnit timeunit, Data keyData, Data valueData) {
        boolean putInternal;
        try {
            putInternal = super.tryPutInternal(timeout, timeunit, keyData, valueData);
        } finally {
            invalidateNearCache(keyData);
        }
        return putInternal;
    }

    @Override
    protected V putInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        V previousValue;
        try {
            previousValue = super.putInternal(ttl, timeunit, keyData, valueData);
        } finally {
            invalidateNearCache(keyData);
        }
        return previousValue;
    }

    @Override
    protected void putTransientInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        try {
            super.putTransientInternal(ttl, timeunit, keyData, valueData);
        } finally {
            invalidateNearCache(keyData);
        }
    }

    @Override
    protected V putIfAbsentInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        V previousValue;
        try {
            previousValue = super.putIfAbsentInternal(ttl, timeunit, keyData, valueData);
        } finally {
            invalidateNearCache(keyData);
        }
        return previousValue;
    }

    @Override
    protected boolean replaceIfSameInternal(Data keyData, Data oldValueData, Data newValueData) {
        boolean replaceIfSame;
        try {
            replaceIfSame = super.replaceIfSameInternal(keyData, oldValueData, newValueData);
        } finally {
            invalidateNearCache(keyData);
        }
        return replaceIfSame;
    }

    @Override
    protected V replaceInternal(Data keyData, Data valueData) {
        V v;
        try {
            v = super.replaceInternal(keyData, valueData);
        } finally {
            invalidateNearCache(keyData);
        }
        return v;
    }

    @Override
    protected void setInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        try {
            super.setInternal(ttl, timeunit, keyData, valueData);
        } finally {
            invalidateNearCache(keyData);
        }
    }

    @Override
    protected boolean evictInternal(Data keyData) {
        boolean evicted;
        try {
            evicted = super.evictInternal(keyData);
        } finally {
            invalidateNearCache(keyData);
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
    protected void loadAllInternal(boolean replaceExistingValues, Collection<Data> dataKeys) {
        try {
            super.loadAllInternal(replaceExistingValues, dataKeys);
        } finally {
            invalidateNearCache(dataKeys);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<MapGetAllCodec.ResponseParameters> getAllInternal(Map<Integer, List<Data>> pIdToKeyData, Map<K, V> result) {
        Map<Data, Long> reservations = new HashMap<Data, Long>();
        List<MapGetAllCodec.ResponseParameters> responses;
        try {
            for (Entry<Integer, List<Data>> partitionKeyEntry : pIdToKeyData.entrySet()) {
                List<Data> keyList = partitionKeyEntry.getValue();
                Iterator<Data> iterator = keyList.iterator();
                while (iterator.hasNext()) {
                    Data key = iterator.next();
                    Object value = getCachedValue(key, true);

                    if (value == null || value == NOT_CACHED) {
                        long reservationId = nearCache.tryReserveForUpdate(key);
                        if (reservationId != NOT_RESERVED) {
                            reservations.put(key, reservationId);
                        }
                        continue;
                    }
                    result.put((K) toObject(key), (V) toObject(value));
                    iterator.remove();
                }
            }

            responses = super.getAllInternal(pIdToKeyData, result);
            for (MapGetAllCodec.ResponseParameters resultParameters : responses) {
                for (Entry<Data, Data> entry : resultParameters.response) {
                    Data key = entry.getKey();
                    Data value = entry.getValue();

                    Long reservationId = reservations.get(key);
                    if (reservationId != null) {
                        Object cachedValue = tryPublishReserved(key, value, reservationId);
                        result.put((K) toObject(key), (V) toObject(cachedValue));
                        reservations.remove(key);
                    }
                }
            }
        } finally {
            releaseRemainingReservedKeys(reservations);
        }
        return responses;
    }

    private void releaseRemainingReservedKeys(Map<Data, Long> reservedKeys) {
        for (Entry<Data, Long> entry : reservedKeys.entrySet()) {
            invalidateNearCache(entry.getKey());
        }
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        LocalMapStats localMapStats = super.getLocalMapStats();
        NearCacheStats nearCacheStats = nearCache.getNearCacheStats();
        ((LocalMapStatsImpl) localMapStats).setNearCacheStats(nearCacheStats);
        return localMapStats;
    }

    @Override
    public Object executeOnKeyInternal(Data keyData, EntryProcessor entryProcessor) {
        Object response;
        try {
            response = super.executeOnKeyInternal(keyData, entryProcessor);
        } finally {
            invalidateNearCache(keyData);
        }
        return response;
    }

    @Override
    public ICompletableFuture submitToKeyInternal(Data keyData, EntryProcessor entryProcessor) {
        ICompletableFuture future;
        try {
            future = super.submitToKeyInternal(keyData, entryProcessor);
        } finally {
            invalidateNearCache(keyData);
        }
        return future;
    }

    @Override
    public void submitToKeyInternal(Data keyData, EntryProcessor entryProcessor, ExecutionCallback callback) {
        try {
            super.submitToKeyInternal(keyData, entryProcessor, callback);
        } finally {
            invalidateNearCache(keyData);
        }
    }

    @Override
    protected Map<K, Object> prepareResult(Collection<Entry<Data, Data>> entrySet) {
        if (CollectionUtil.isEmpty(entrySet)) {
            return emptyMap();
        }
        Map<K, Object> result = MapUtil.createHashMap(entrySet.size());
        for (Entry<Data, Data> entry : entrySet) {
            Data dataKey = entry.getKey();
            invalidateNearCache(dataKey);

            K key = toObject(dataKey);
            Object value = toObject(entry.getValue());
            result.put(key, value);
        }
        return result;
    }

    @Override
    protected void putAllInternal(Map<Integer, List<Map.Entry<Data, Data>>> entryMap) {
        try {
            super.putAllInternal(entryMap);
        } finally {
            for (List<Entry<Data, Data>> entries : entryMap.values()) {
                for (Entry<Data, Data> entry : entries) {
                    invalidateNearCache(entry.getKey());
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

    private Object tryPublishReserved(Data key, Object value, long reservationId) {
        assert value != NOT_CACHED;

        Object cachedValue = nearCache.tryPublishReserved(key, value, reservationId, true);
        return cachedValue != null ? cachedValue : value;
    }

    private Object getCachedValue(Data key, boolean deserializeValue) {
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

    private void invalidateNearCache(Data key) {
        nearCache.remove(key);
    }

    private void invalidateNearCache(Collection<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        for (Data key : keys) {
            nearCache.remove(key);
        }
    }

    public void addNearCacheInvalidationListener(EventHandler handler) {
        try {
            invalidationListenerId = registerListener(createNearCacheEntryListenerCodec(), handler);
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
            ClientContext clientContext = getClientContext();
            return clientContext.getRepairingTask(SERVICE_NAME);
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
                nearCache.remove(key);
            }
        }

        @Override
        public void handle(Collection<Data> keys, Collection<String> sourceUuids,
                           Collection<UUID> partitionUuids, Collection<Long> sequences) {
            for (Data key : keys) {
                nearCache.remove(key);
            }
        }
    }

    // used in tests
    public ClientContext getClientContext() {
        return getContext();
    }

    private int getConnectedServerVersion() {
        ClientContext clientContext = getClientContext();
        ClientClusterService clusterService = clientContext.getClusterService();
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
