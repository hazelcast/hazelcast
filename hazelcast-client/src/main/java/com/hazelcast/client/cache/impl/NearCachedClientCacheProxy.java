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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.executor.CompletedFuture;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.instance.BuildInfo.UNKNOWN_HAZELCAST_VERSION;
import static com.hazelcast.instance.BuildInfo.calculateVersion;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.String.format;

/**
 * An {@link ICacheInternal} implementation which handles Near Cache specific behaviour of methods.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity", "checkstyle:anoninnerlength"})
public class NearCachedClientCacheProxy<K, V> extends ClientCacheProxy<K, V> {

    // eventually consistent Near Cache can only be used with server versions >= 3.8
    private final int minConsistentNearCacheSupportingServerVersion = calculateVersion("3.8");

    private boolean cacheOnUpdate;
    private boolean invalidateOnChange;

    private NearCacheManager nearCacheManager;
    private NearCache<Data, Object> nearCache;
    private String nearCacheMembershipRegistrationId;

    NearCachedClientCacheProxy(CacheConfig<K, V> cacheConfig) {
        super(cacheConfig);
    }

    // for testing only
    public NearCache getNearCache() {
        return nearCache;
    }

    @Override
    public ClientContext getClientContext() {
        return clientContext;
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        ClientConfig clientConfig = clientContext.getClientConfig();
        NearCacheConfig nearCacheConfig = checkNearCacheConfig(clientConfig.getNearCacheConfig(name),
                clientConfig.getNativeMemoryConfig());
        cacheOnUpdate = isCacheOnUpdate(nearCacheConfig, nameWithPrefix, logger);
        invalidateOnChange = nearCacheConfig.isInvalidateOnChange();

        ICacheDataStructureAdapter<K, V> adapter = new ICacheDataStructureAdapter<K, V>(this);
        nearCacheManager = clientContext.getNearCacheManager();
        nearCache = nearCacheManager.getOrCreateNearCache(nameWithPrefix, nearCacheConfig, adapter);
        CacheStatistics localCacheStatistics = super.getLocalCacheStatistics();
        ((ClientCacheStatisticsImpl) localCacheStatistics).setNearCacheStats(nearCache.getNearCacheStats());

        registerInvalidationListener();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V getSyncInternal(Data keyData, ExpiryPolicy expiryPolicy) {
        V value = (V) getCachedValue(keyData, true);
        if (value != NOT_CACHED) {
            return value;
        }

        try {
            long reservationId = nearCache.tryReserveForUpdate(keyData);
            value = super.getSyncInternal(keyData, expiryPolicy);
            if (reservationId != NOT_RESERVED) {
                value = tryPublishReserved(keyData, value, reservationId);
            }
            return value;
        } catch (Throwable throwable) {
            invalidateNearCache(keyData);
            throw rethrow(throwable);
        }
    }

    @Override
    protected InternalCompletableFuture<V> getAsyncInternal(Data dataKey, ExpiryPolicy expiryPolicy,
                                                            ExecutionCallback<V> callback) {
        Object value = getCachedValue(dataKey, false);
        if (value != NOT_CACHED) {
            return new CompletedFuture<V>(clientContext.getSerializationService(), value,
                    clientContext.getExecutionService().getUserExecutor());
        }

        try {
            long reservationId = nearCache.tryReserveForUpdate(dataKey);
            GetAsyncCallback getAsyncCallback = new GetAsyncCallback(dataKey, reservationId, callback);
            return super.getAsyncInternal(dataKey, expiryPolicy, getAsyncCallback);
        } catch (Throwable t) {
            invalidateNearCache(dataKey);
            throw rethrow(t);
        }
    }

    @Override
    protected void onPutSyncInternal(Data keyData, Data valueData, V value) {
        try {
            super.onPutSyncInternal(keyData, valueData, value);
        } finally {
            cacheOrInvalidate(keyData, valueData, value);
        }
    }

    @Override
    protected void onPutIfAbsentSyncInternal(Data keyData, Data valueData, V value) {
        try {
            super.onPutIfAbsentSyncInternal(keyData, valueData, value);
        } finally {
            cacheOrInvalidate(keyData, valueData, value);
        }
    }

    @Override
    protected void onPutIfAbsentAsyncInternal(Data keyData, Data valueData, V value,
                                              ClientDelegatingFuture<Boolean> delegatingFuture,
                                              ExecutionCallback<Boolean> callback) {
        CacheOrInvalidateCallback<Boolean> wrapped = new CacheOrInvalidateCallback<Boolean>(keyData, valueData, value, callback);
        super.onPutIfAbsentAsyncInternal(keyData, valueData, value, delegatingFuture, wrapped);
    }

    @Override
    protected ClientDelegatingFuture<V> wrapPutAsyncFuture(Data keyData, Data valueData, V value,
                                                           ClientInvocationFuture invocationFuture,
                                                           OneShotExecutionCallback<V> callback) {
        PutAsyncOneShotCallback nearCachePopulator = new PutAsyncOneShotCallback(keyData, valueData, value, callback);
        return super.wrapPutAsyncFuture(keyData, valueData, value, invocationFuture, nearCachePopulator);
    }

    @Override
    protected void onGetAndRemoveAsyncInternal(ClientDelegatingFuture future, ExecutionCallback callback, Data keyData) {
        try {
            super.onGetAndRemoveAsyncInternal(future, callback, keyData);
        } finally {
            invalidateNearCache(keyData);
        }
    }

    @Override
    protected <T> void onReplaceInternalAsync(Data keyData, Data valueData, V value,
                                              ClientDelegatingFuture<T> delegatingFuture, ExecutionCallback<T> callback) {
        CacheOrInvalidateCallback<T> wrapped = new CacheOrInvalidateCallback<T>(keyData, valueData, value, callback);
        super.onReplaceInternalAsync(keyData, valueData, value, delegatingFuture, wrapped);
    }

    @Override
    protected <T> void onReplaceAndGetAsync(Data keyData, Data valueData, V value,
                                            ClientDelegatingFuture<T> delegatingFuture, ExecutionCallback<T> callback) {
        CacheOrInvalidateCallback<T> wrapped = new CacheOrInvalidateCallback<T>(keyData, valueData, value, callback);
        super.onReplaceAndGetAsync(keyData, valueData, value, delegatingFuture, wrapped);
    }

    @Override
    protected Map<K, V> getAllInternal(Collection<Data> dataKeys, ExpiryPolicy expiryPolicy, Map<K, V> resultMap,
                                       long startNanos) {
        populateResultFromNearCache(dataKeys, resultMap);
        if (dataKeys.isEmpty()) {
            return resultMap;
        }

        Map<Data, Long> reservations = createHashMap(dataKeys.size());
        for (Object keyData : dataKeys) {
            long reservationId = tryReserveForUpdate((Data) keyData);
            if (reservationId != NOT_RESERVED) {
                reservations.put((Data) keyData, reservationId);
            }
        }

        try {
            Map<K, V> all = super.getAllInternal(dataKeys, expiryPolicy, resultMap, startNanos);
            for (Map.Entry<K, V> entry : all.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();

                Data keyData = toData(key);
                Long reservationId = reservations.get(keyData);
                if (reservationId != null) {
                    V cachedValue = tryPublishReserved(keyData, value, reservationId);
                    V newValue = toObject(cachedValue);
                    resultMap.put(key, newValue);
                    reservations.remove(keyData);
                }
            }
        } finally {
            releaseRemainingReservedKeys(reservations);
        }
        return resultMap;
    }

    @Override
    protected void putAllInternal(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy,
                                  List<Map.Entry<Data, Data>>[] entriesPerPartition, long startNanos) {
        try {
            super.putAllInternal(map, expiryPolicy, entriesPerPartition, startNanos);
            // cache or invalidate Near Cache
            for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
                List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
                if (entries != null) {
                    for (Map.Entry<Data, Data> entry : entries) {
                        cacheOrInvalidate(entry.getKey(), entry.getValue(), null);
                    }
                }
            }
        } catch (Throwable t) {
            for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
                List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
                if (entries != null) {
                    for (Map.Entry<Data, Data> entry : entries) {
                        invalidateNearCache(entry.getKey());
                    }
                }
            }
            throw rethrow(t);
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
    protected void loadAllInternal(List<Data> dataKeys, boolean replaceExistingValues, CompletionListener completionListener) {
        try {
            super.loadAllInternal(dataKeys, replaceExistingValues, completionListener);
        } finally {
            for (Object dataKey : dataKeys) {
                invalidateNearCache((Data) dataKey);
            }
        }
    }

    @Override
    protected void removeAllKeysInternal(Collection<Data> dataKeys, long startNanos) {
        try {
            super.removeAllKeysInternal(dataKeys, startNanos);
        } finally {
            for (Object dataKey : dataKeys) {
                invalidateNearCache((Data) dataKey);
            }
        }
    }

    @Override
    public void onRemoveSyncInternal(Data keyData) {
        try {
            super.onRemoveSyncInternal(keyData);
        } finally {
            invalidateNearCache(keyData);
        }
    }

    @Override
    protected void onRemoveAsyncInternal(Data keyData, ClientDelegatingFuture future, ExecutionCallback callback) {
        try {
            super.onRemoveAsyncInternal(keyData, future, callback);
        } finally {
            invalidateNearCache(keyData);
        }
    }

    @Override
    public void removeAll() {
        try {
            super.removeAll();
        } finally {
            nearCache.clear();
        }
    }

    @Override
    public void clear() {
        try {
            super.clear();
        } finally {
            nearCache.clear();
        }
    }

    @Override
    protected Object invokeInternal(Data keyData, Data epData, Object[] arguments) {
        try {
            return super.invokeInternal(keyData, epData, arguments);
        } finally {
            invalidateNearCache(keyData);
        }
    }

    @Override
    public void close() {
        removeInvalidationListener();
        nearCacheManager.clearNearCache(nearCache.getName());

        super.close();
    }

    @Override
    protected void onDestroy() {
        removeInvalidationListener();
        nearCacheManager.destroyNearCache(nearCache.getName());

        super.onDestroy();
    }

    @SuppressWarnings("unchecked")
    private void populateResultFromNearCache(Collection<Data> keys, Map<K, V> result) {
        Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            Data key = iterator.next();
            Object cached = getCachedValue(key, true);
            if (cached != NOT_CACHED) {
                result.put((K) toObject(key), (V) cached);
                iterator.remove();
            }
        }
    }

    private Object getCachedValue(Data keyData, boolean deserializeValue) {
        Object cached = nearCache.get(keyData);
        // caching null values is not supported for ICache Near Cache
        assert cached != CACHED_AS_NULL;

        if (cached == null) {
            return NOT_CACHED;
        }
        return deserializeValue ? toObject(cached) : cached;
    }

    @SuppressWarnings("unchecked")
    private void cacheOrInvalidate(Data keyData, Data valueData, V value) {
        if (cacheOnUpdate) {
            V valueToStore = (V) nearCache.selectToSave(valueData, value);
            nearCache.put(keyData, valueToStore);
        } else {
            invalidateNearCache(keyData);
        }
    }

    private void invalidateNearCache(Data key) {
        assert key != null;

        nearCache.remove(key);
    }

    private long tryReserveForUpdate(Data keyData) {
        return nearCache.tryReserveForUpdate(keyData);
    }

    /**
     * Publishes value got from remote or deletes reserved record when remote value is {@code null}.
     *
     * @param key           key to update in Near Cache
     * @param remoteValue   fetched value from server
     * @param reservationId reservation id for this key
     * @param deserialize   deserialize returned value
     * @return last known value for the key
     */
    @SuppressWarnings("unchecked")
    private V tryPublishReserved(Data key, V remoteValue, long reservationId, boolean deserialize) {
        assert remoteValue != NOT_CACHED;

        // caching null value is not supported for ICache Near Cache
        if (remoteValue == null) {
            // needed to delete reserved record
            invalidateNearCache(key);
            return null;
        }

        V cachedValue = null;
        if (reservationId != NOT_RESERVED) {
            cachedValue = (V) nearCache.tryPublishReserved(key, remoteValue, reservationId, deserialize);
        }
        return cachedValue == null ? remoteValue : cachedValue;
    }

    private V tryPublishReserved(Data key, V remoteValue, long reservationId) {
        return tryPublishReserved(key, remoteValue, reservationId, true);
    }

    private void releaseRemainingReservedKeys(Map<Data, Long> reservedKeys) {
        for (Data key : reservedKeys.keySet()) {
            nearCache.remove(key);
        }
    }

    private void registerInvalidationListener() {
        if (!invalidateOnChange) {
            return;
        }

        ListenerMessageCodec listenerCodec = createInvalidationListenerCodec();
        ClientListenerService listenerService = clientContext.getListenerService();

        EventHandler eventHandler = createInvalidationEventHandler();
        nearCacheMembershipRegistrationId = listenerService.registerListener(listenerCodec, eventHandler);
    }

    private EventHandler createInvalidationEventHandler() {
        return new ConnectedServerVersionAwareNearCacheEventHandler();
    }

    private ListenerMessageCodec createInvalidationListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                if (supportsRepairableNearCache()) {
                    // this is for servers >= 3.8
                    return CacheAddNearCacheInvalidationListenerCodec.encodeRequest(nameWithPrefix, localOnly);
                }
                // this is for servers < 3.8
                return CacheAddInvalidationListenerCodec.encodeRequest(nameWithPrefix, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                if (supportsRepairableNearCache()) {
                    // this is for servers >= 3.8
                    return CacheAddNearCacheInvalidationListenerCodec.decodeResponse(clientMessage).response;
                }
                // this is for servers < 3.8
                return CacheAddInvalidationListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return CacheRemoveEntryListenerCodec.encodeRequest(nameWithPrefix, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return CacheRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    private int getConnectedServerVersion() {
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

    private void removeInvalidationListener() {
        if (!invalidateOnChange) {
            return;
        }

        String registrationId = nearCacheMembershipRegistrationId;
        if (registrationId != null) {
            clientContext.getRepairingTask(SERVICE_NAME).deregisterHandler(name);
            clientContext.getListenerService().deregisterListener(registrationId);
        }
    }

    @SuppressWarnings("deprecation")
    static boolean isCacheOnUpdate(NearCacheConfig nearCacheConfig, String cacheName, ILogger logger) {
        NearCacheConfig.LocalUpdatePolicy localUpdatePolicy = nearCacheConfig.getLocalUpdatePolicy();
        if (localUpdatePolicy == CACHE) {
            logger.warning(format("Deprecated local update policy is found for cache `%s`."
                            + " The policy `%s` is subject to remove in further releases. Instead you can use `%s`",
                    cacheName, CACHE, CACHE_ON_UPDATE));
            return true;
        }

        return localUpdatePolicy == CACHE_ON_UPDATE;
    }

    private static NearCacheConfig checkNearCacheConfig(NearCacheConfig nearCacheConfig, NativeMemoryConfig nativeMemoryConfig) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat != NATIVE) {
            return nearCacheConfig;
        }

        checkTrue(nativeMemoryConfig.isEnabled(), "Enable native memory config to use NATIVE in-memory-format for Near Cache");
        return nearCacheConfig;
    }

    private final class GetAsyncCallback implements ExecutionCallback<V> {

        private final Data keyData;
        private final long reservationId;
        private final ExecutionCallback<V> callback;

        GetAsyncCallback(Data keyData, long reservationId, ExecutionCallback<V> callback) {
            this.keyData = keyData;
            this.reservationId = reservationId;
            this.callback = callback;
        }

        @Override
        public void onResponse(V valueData) {
            try {
                if (callback != null) {
                    callback.onResponse(valueData);
                }
            } finally {
                tryPublishReserved(keyData, valueData, reservationId, false);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            try {
                if (callback != null) {
                    callback.onFailure(t);
                }
            } finally {
                invalidateNearCache(keyData);
            }
        }
    }

    private final class PutAsyncOneShotCallback extends OneShotExecutionCallback<V> {

        private final Data keyData;
        private final Data newValueData;
        private final V newValue;
        private final OneShotExecutionCallback<V> statsCallback;

        private PutAsyncOneShotCallback(Data keyData, Data newValueData, V newValue, OneShotExecutionCallback<V> callback) {
            this.keyData = keyData;
            this.newValueData = newValueData;
            this.newValue = newValue;
            this.statsCallback = callback;
        }

        @Override
        protected void onResponseInternal(V response) {
            try {
                if (statsCallback != null) {
                    statsCallback.onResponseInternal(response);
                }
            } finally {
                cacheOrInvalidate(keyData, newValueData, newValue);
            }
        }

        @Override
        protected void onFailureInternal(Throwable t) {
            try {
                if (statsCallback != null) {
                    statsCallback.onFailureInternal(t);
                }
            } finally {
                invalidateNearCache(keyData);
            }
        }
    }

    private final class CacheOrInvalidateCallback<T> implements ExecutionCallback<T> {

        private final Data keyData;
        private final Data valueData;
        private final V value;
        private final ExecutionCallback<T> callback;

        CacheOrInvalidateCallback(Data keyData, Data valueData, V value, ExecutionCallback<T> callback) {
            this.callback = callback;
            this.keyData = keyData;
            this.valueData = valueData;
            this.value = value;
        }

        @Override
        public void onResponse(T response) {
            try {
                if (callback != null) {
                    callback.onResponse(response);
                }
            } finally {
                cacheOrInvalidate(keyData, valueData, value);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            try {
                if (callback != null) {
                    callback.onFailure(t);
                }
            } finally {
                invalidateNearCache(keyData);
            }
        }
    }

    /**
     * Deals with client compatibility.
     * <p>
     * Eventual consistency for Near Cache can be used with server versions >= 3.8,
     * other connected server versions must use {@link Pre38NearCacheEventHandler}.
     */
    private final class ConnectedServerVersionAwareNearCacheEventHandler implements EventHandler<ClientMessage> {

        private final RepairableNearCacheEventHandler repairingEventHandler = new RepairableNearCacheEventHandler();
        private final Pre38NearCacheEventHandler pre38EventHandler = new Pre38NearCacheEventHandler();
        private volatile boolean supportsRepairableNearCache;

        @Override
        public void beforeListenerRegister() {
            pre38EventHandler.beforeListenerRegister();
            repairingEventHandler.beforeListenerRegister();
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
     * For repairing functionality please see {@link RepairingHandler}
     */
    private final class RepairableNearCacheEventHandler
            extends CacheAddNearCacheInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private volatile RepairingHandler repairingHandler;

        @Override
        public void beforeListenerRegister() {
            nearCache.clear();
            getRepairingTask().deregisterHandler(nameWithPrefix);
        }

        @Override
        public void onListenerRegister() {
            nearCache.clear();
            repairingHandler = getRepairingTask().registerAndGetHandler(nameWithPrefix, nearCache);
        }

        @Override
        public void handle(String name, Data key, String sourceUuid, UUID partitionUuid, long sequence) {
            repairingHandler.handle(key, sourceUuid, partitionUuid, sequence);
        }

        @Override
        public void handle(String name, Collection<Data> keys, Collection<String> sourceUuids,
                           Collection<UUID> partitionUuids, Collection<Long> sequences) {
            repairingHandler.handle(keys, sourceUuids, partitionUuids, sequences);
        }

        private RepairingTask getRepairingTask() {
            return clientContext.getRepairingTask(CacheService.SERVICE_NAME);
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
            extends CacheAddInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private String clientUuid;

        private Pre38NearCacheEventHandler() {
            this.clientUuid = clientContext.getClusterService().getLocalClient().getUuid();
        }

        @Override
        public void handle(String name, Data key, String sourceUuid, UUID partitionUuid, long sequence) {
            if (clientUuid.equals(sourceUuid)) {
                return;
            }
            if (key != null) {
                nearCache.remove(key);
            } else {
                nearCache.clear();
            }
        }

        @Override
        public void handle(String name, Collection<Data> keys, Collection<String> sourceUuids,
                           Collection<UUID> partitionUuids, Collection<Long> sequences) {
            if (sourceUuids != null && !sourceUuids.isEmpty()) {
                Iterator<Data> keysIt = keys.iterator();
                Iterator<String> sourceUuidsIt = sourceUuids.iterator();
                while (keysIt.hasNext() && sourceUuidsIt.hasNext()) {
                    Data key = keysIt.next();
                    String sourceUuid = sourceUuidsIt.next();
                    if (!clientUuid.equals(sourceUuid)) {
                        nearCache.remove(key);
                    }
                }
            } else {
                for (Data key : keys) {
                    nearCache.remove(key);
                }
            }
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
            nearCache.clear();
        }
    }
}
