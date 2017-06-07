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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.cache.impl.CacheProxyUtil.NULL_KEY_IS_NOT_ALLOWED;
import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.instance.BuildInfo.UNKNOWN_HAZELCAST_VERSION;
import static com.hazelcast.instance.BuildInfo.calculateVersion;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.util.CollectionUtil.objectToDataCollection;
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
    private boolean serializeKeys;

    private NearCacheManager nearCacheManager;
    private NearCache<Object, Object> nearCache;
    private String nearCacheMembershipRegistrationId;

    NearCachedClientCacheProxy(CacheConfig<K, V> cacheConfig, ClientContext context) {
        super(cacheConfig, context);
    }

    // for testing only
    public NearCache getNearCache() {
        return nearCache;
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        ClientConfig clientConfig = getContext().getClientConfig();
        NearCacheConfig nearCacheConfig = checkNearCacheConfig(clientConfig.getNearCacheConfig(name),
                clientConfig.getNativeMemoryConfig());
        cacheOnUpdate = isCacheOnUpdate(nearCacheConfig, nameWithPrefix, logger);
        invalidateOnChange = nearCacheConfig.isInvalidateOnChange();
        serializeKeys = nearCacheConfig.isSerializeKeys();

        ICacheDataStructureAdapter<K, V> adapter = new ICacheDataStructureAdapter<K, V>(this);
        nearCacheManager = getContext().getNearCacheManager();
        nearCache = nearCacheManager.getOrCreateNearCache(nameWithPrefix, nearCacheConfig, adapter);
        CacheStatistics localCacheStatistics = super.getLocalCacheStatistics();
        ((ClientCacheStatisticsImpl) localCacheStatistics).setNearCacheStats(nearCache.getNearCacheStats());

        registerInvalidationListener();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V getSyncInternal(Object key, ExpiryPolicy expiryPolicy) {
        key = serializeKeys ? toData(key) : key;
        V value = (V) getCachedValue(key, true);
        if (value != NOT_CACHED) {
            return value;
        }

        try {
            long reservationId = nearCache.tryReserveForUpdate(key);
            Data keyData = toData(key);
            value = super.getSyncInternal(keyData, expiryPolicy);
            if (reservationId != NOT_RESERVED) {
                value = tryPublishReserved(key, keyData, value, reservationId);
            }
            return value;
        } catch (Throwable throwable) {
            invalidateNearCache(key);
            throw rethrow(throwable);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected InternalCompletableFuture<V> getAsyncInternal(Object key, ExpiryPolicy expiryPolicy,
                                                            ExecutionCallback<V> callback) {
        key = serializeKeys ? toData(key) : key;
        V value = (V) getCachedValue(key, false);
        if (value != NOT_CACHED) {
            return new CompletedFuture<V>(getSerializationService(), value, getContext().getExecutionService().getUserExecutor());
        }

        try {
            long reservationId = nearCache.tryReserveForUpdate(key);
            Data keyData = toData(key);
            GetAsyncCallback getAsyncCallback = new GetAsyncCallback(key, keyData, reservationId, callback);
            return super.getAsyncInternal(keyData, expiryPolicy, getAsyncCallback);
        } catch (Throwable t) {
            invalidateNearCache(key);
            throw rethrow(t);
        }
    }

    @Override
    protected void onPutSyncInternal(K key, V value, Data keyData, Data valueData) {
        try {
            super.onPutSyncInternal(key, value, keyData, valueData);
        } finally {
            cacheOrInvalidate(keyData, serializeKeys ? keyData : key, valueData, value);
        }
    }

    @Override
    protected void onPutIfAbsentSyncInternal(K key, V value, Data keyData, Data valueData) {
        try {
            super.onPutIfAbsentSyncInternal(key, value, keyData, valueData);
        } finally {
            cacheOrInvalidate(keyData, serializeKeys ? keyData : key, valueData, value);
        }
    }

    @Override
    protected void onPutIfAbsentAsyncInternal(K key, V value, Data keyData, Data valueData,
                                              ClientDelegatingFuture<Boolean> delegatingFuture,
                                              ExecutionCallback<Boolean> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        CacheOrInvalidateCallback<Boolean> wrapped = new CacheOrInvalidateCallback<Boolean>(callbackKey, keyData, valueData,
                value, callback);
        super.onPutIfAbsentAsyncInternal(key, value, keyData, valueData, delegatingFuture, wrapped);
    }

    @Override
    protected ClientDelegatingFuture<V> wrapPutAsyncFuture(K key, V value, Data keyData, Data valueData,
                                                           ClientInvocationFuture invocationFuture,
                                                           OneShotExecutionCallback<V> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        PutAsyncOneShotCallback putAsyncCallback = new PutAsyncOneShotCallback(callbackKey, keyData, valueData, value, callback);
        return super.wrapPutAsyncFuture(key, value, keyData, valueData, invocationFuture, putAsyncCallback);
    }

    @Override
    protected <T> void onGetAndRemoveAsyncInternal(K key, Data keyData, ClientDelegatingFuture<T> delegatingFuture,
                                                   ExecutionCallback<T> callback) {
        try {
            super.onGetAndRemoveAsyncInternal(key, keyData, delegatingFuture, callback);
        } finally {
            invalidateNearCache(serializeKeys ? keyData : key);
        }
    }

    @Override
    protected <T> void onReplaceInternalAsync(K key, V value, Data keyData, Data valueData,
                                              ClientDelegatingFuture<T> delegatingFuture, ExecutionCallback<T> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        CacheOrInvalidateCallback<T> wrapped = new CacheOrInvalidateCallback<T>(callbackKey, keyData, valueData, value, callback);
        super.onReplaceInternalAsync(key, value, keyData, valueData, delegatingFuture, wrapped);
    }

    @Override
    protected <T> void onReplaceAndGetAsync(K key, V value, Data keyData, Data valueData,
                                            ClientDelegatingFuture<T> delegatingFuture, ExecutionCallback<T> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        CacheOrInvalidateCallback<T> wrapped = new CacheOrInvalidateCallback<T>(callbackKey, keyData, valueData, value, callback);
        super.onReplaceAndGetAsync(key, value, keyData, valueData, delegatingFuture, wrapped);
    }

    @Override
    protected List<Map.Entry<Data, Data>> getAllInternal(Set<? extends K> keys, Collection<Data> dataKeys,
                                                         ExpiryPolicy expiryPolicy, Map<K, V> resultMap, long startNanos) {
        if (keys.isEmpty()) {
            return Collections.emptyList();
        }
        if (serializeKeys) {
            objectToDataCollection(keys, dataKeys, getSerializationService(), NULL_KEY_IS_NOT_ALLOWED);
        }
        Collection<?> keySet = serializeKeys ? dataKeys : keys;
        populateResultFromNearCache(keySet, resultMap);

        Map<Object, Long> reservations = createHashMap(keys.size());
        for (Object key : keySet) {
            long reservationId = tryReserveForUpdate(key);
            if (reservationId != NOT_RESERVED) {
                reservations.put(key, reservationId);
            }
        }

        try {
            List<Map.Entry<Data, Data>> response = super.getAllInternal(keys, dataKeys, expiryPolicy, resultMap, startNanos);
            populateResultFromRemote(resultMap, reservations, response);
            return response;
        } finally {
            releaseRemainingReservedKeys(reservations);
        }
    }

    @SuppressWarnings("unchecked")
    private void populateResultFromNearCache(Collection<?> keys, Map<K, V> result) {
        Iterator<?> iterator = keys.iterator();
        while (iterator.hasNext()) {
            Object key = iterator.next();
            Object cached = getCachedValue(key, true);
            if (cached != NOT_CACHED) {
                result.put((K) (serializeKeys ? toObject(key) : key), (V) cached);
                iterator.remove();
            }
        }
    }

    private void populateResultFromRemote(Map<K, V> resultMap, Map<Object, Long> reservations,
                                          List<Map.Entry<Data, Data>> response) {
        if (serializeKeys) {
            for (Map.Entry<Data, Data> entry : response) {
                Data keyData = entry.getKey();
                Long reservationId = reservations.get(keyData);
                if (reservationId != null) {
                    K key = toObject(keyData);
                    V remoteValue = resultMap.get(key);
                    V cachedValue = tryPublishReserved(keyData, keyData, remoteValue, reservationId);
                    V newValue = toObject(cachedValue);
                    resultMap.put(key, newValue);
                    reservations.remove(keyData);
                }
            }
        } else {
            for (Map.Entry<K, V> entry : resultMap.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                Long reservationId = reservations.get(key);
                if (reservationId != null) {
                    V cachedValue = tryPublishReserved(key, toData(key), value, reservationId);
                    V newValue = toObject(cachedValue);
                    resultMap.put(key, newValue);
                    reservations.remove(key);
                }
            }
        }
    }

    @Override
    protected void putAllInternal(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy,
                                  List<Map.Entry<Data, Data>>[] entriesPerPartition, long startNanos) {
        try {
            super.putAllInternal(map, expiryPolicy, entriesPerPartition, startNanos);
            cacheOrInvalidate(map, entriesPerPartition, true);
        } catch (Throwable t) {
            cacheOrInvalidate(map, entriesPerPartition, false);
            throw rethrow(t);
        }
    }

    private void cacheOrInvalidate(Map<? extends K, ? extends V> map, List<Map.Entry<Data, Data>>[] entriesPerPartition,
                                   boolean isCacheOrInvalidate) {
        if (serializeKeys) {
            for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
                List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
                if (entries != null) {
                    for (Map.Entry<Data, Data> entry : entries) {
                        if (isCacheOrInvalidate) {
                            Data key = entry.getKey();
                            cacheOrInvalidate(key, key, entry.getValue(), null);
                        } else {
                            invalidateNearCache(entry.getKey());
                        }
                    }
                }
            }
        } else {
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                if (isCacheOrInvalidate) {
                    cacheOrInvalidate(null, entry.getKey(), null, entry.getValue());
                } else {
                    invalidateNearCache(entry.getKey());
                }
            }
        }
    }

    @Override
    protected boolean containsKeyInternal(Object key) {
        key = serializeKeys ? toData(key) : key;
        Object cached = getCachedValue(key, false);
        if (cached != NOT_CACHED) {
            return true;
        }
        return super.containsKeyInternal(key);
    }

    @Override
    protected void loadAllInternal(Set<? extends K> keys, List<Data> dataKeys, boolean replaceExistingValues,
                                   CompletionListener completionListener) {
        try {
            super.loadAllInternal(keys, dataKeys, replaceExistingValues, completionListener);
        } finally {
            if (serializeKeys) {
                for (Data dataKey : dataKeys) {
                    invalidateNearCache(dataKey);
                }
            } else {
                for (K key : keys) {
                    invalidateNearCache(key);
                }
            }
        }
    }

    @Override
    protected void removeAllKeysInternal(Set<? extends K> keys, Collection<Data> dataKeys, long startNanos) {
        try {
            super.removeAllKeysInternal(keys, dataKeys, startNanos);
        } finally {
            if (serializeKeys) {
                for (Data dataKey : dataKeys) {
                    invalidateNearCache(dataKey);
                }
            } else {
                for (K key : keys) {
                    invalidateNearCache(key);
                }
            }
        }
    }

    @Override
    public void onRemoveSyncInternal(Object key, Data keyData) {
        try {
            super.onRemoveSyncInternal(key, keyData);
        } finally {
            invalidateNearCache(serializeKeys ? keyData : key);
        }
    }

    @Override
    protected void onRemoveAsyncInternal(Object key, Data keyData, ClientDelegatingFuture future, ExecutionCallback callback) {
        try {
            super.onRemoveAsyncInternal(key, keyData, future, callback);
        } finally {
            invalidateNearCache(serializeKeys ? keyData : key);
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
    protected Object invokeInternal(Object key, Data epData, Object[] arguments) {
        key = serializeKeys ? toData(key) : key;
        try {
            return super.invokeInternal(key, epData, arguments);
        } finally {
            invalidateNearCache(key);
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

    private Object getCachedValue(Object key, boolean deserializeValue) {
        Object cached = nearCache.get(key);
        // caching null values is not supported for ICache Near Cache
        assert cached != CACHED_AS_NULL;

        if (cached == null) {
            return NOT_CACHED;
        }
        return deserializeValue ? toObject(cached) : cached;
    }

    @SuppressWarnings("unchecked")
    private void cacheOrInvalidate(Data keyData, Object key, Data valueData, V value) {
        if (cacheOnUpdate) {
            V valueToStore = (V) nearCache.selectToSave(valueData, value);
            nearCache.put(key, keyData, valueToStore);
        } else {
            invalidateNearCache(key);
        }
    }

    private void invalidateNearCache(Object key) {
        assert key != null;

        nearCache.remove(key);
    }

    private long tryReserveForUpdate(Object key) {
        return nearCache.tryReserveForUpdate(key);
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
    private V tryPublishReserved(Object key, Data keyData, V remoteValue, long reservationId, boolean deserialize) {
        assert remoteValue != NOT_CACHED;

        // caching null value is not supported for ICache Near Cache
        if (remoteValue == null) {
            // needed to delete reserved record
            invalidateNearCache(key);
            return null;
        }

        V cachedValue = null;
        if (reservationId != NOT_RESERVED) {
            cachedValue = (V) nearCache.tryPublishReserved(key, keyData, remoteValue, reservationId, deserialize);
        }
        return cachedValue == null ? remoteValue : cachedValue;
    }

    private V tryPublishReserved(Object key, Data keyData, V remoteValue, long reservationId) {
        return tryPublishReserved(key, keyData, remoteValue, reservationId, true);
    }

    private void releaseRemainingReservedKeys(Map<Object, Long> reservedKeys) {
        for (Object key : reservedKeys.keySet()) {
            nearCache.remove(key);
        }
    }

    public String addNearCacheInvalidationListener(EventHandler eventHandler) {
        return registerListener(createInvalidationListenerCodec(), eventHandler);
    }

    private void registerInvalidationListener() {
        if (!invalidateOnChange) {
            return;
        }

        EventHandler eventHandler = createInvalidationEventHandler();
        nearCacheMembershipRegistrationId = addNearCacheInvalidationListener(eventHandler);
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

    private void removeInvalidationListener() {
        if (!invalidateOnChange) {
            return;
        }

        String registrationId = nearCacheMembershipRegistrationId;
        if (registrationId != null) {
            getContext().getRepairingTask(SERVICE_NAME).deregisterHandler(name);
            getContext().getListenerService().deregisterListener(registrationId);
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

        private final Object key;
        private final Data keyData;
        private final long reservationId;
        private final ExecutionCallback<V> callback;

        GetAsyncCallback(Object key, Data keyData, long reservationId, ExecutionCallback<V> callback) {
            this.key = key;
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
                tryPublishReserved(key, keyData, valueData, reservationId, false);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            try {
                if (callback != null) {
                    callback.onFailure(t);
                }
            } finally {
                invalidateNearCache(key);
            }
        }
    }

    private final class PutAsyncOneShotCallback extends OneShotExecutionCallback<V> {

        private final Object key;
        private final Data keyData;
        private final Data newValueData;
        private final V newValue;
        private final OneShotExecutionCallback<V> statsCallback;

        private PutAsyncOneShotCallback(Object key, Data keyData, Data newValueData, V newValue,
                                        OneShotExecutionCallback<V> callback) {
            this.key = key;
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
                cacheOrInvalidate(keyData, key, newValueData, newValue);
            }
        }

        @Override
        protected void onFailureInternal(Throwable t) {
            try {
                if (statsCallback != null) {
                    statsCallback.onFailureInternal(t);
                }
            } finally {
                invalidateNearCache(key);
            }
        }
    }

    private final class CacheOrInvalidateCallback<T> implements ExecutionCallback<T> {

        private final Object key;
        private final Data keyData;
        private final Data valueData;
        private final V value;
        private final ExecutionCallback<T> callback;

        CacheOrInvalidateCallback(Object key, Data keyData, Data valueData, V value, ExecutionCallback<T> callback) {
            this.keyData = keyData;
            this.callback = callback;
            this.key = key;
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
                cacheOrInvalidate(keyData, key, valueData, value);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            try {
                if (callback != null) {
                    callback.onFailure(t);
                }
            } finally {
                invalidateNearCache(key);
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
            return getContext().getRepairingTask(CacheService.SERVICE_NAME);
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
            this.clientUuid = getContext().getClusterService().getLocalClient().getUuid();
        }

        @Override
        public void handle(String name, Data key, String sourceUuid, UUID partitionUuid, long sequence) {
            if (clientUuid.equals(sourceUuid)) {
                return;
            }
            if (key != null) {
                nearCache.remove(serializeKeys ? key : toObject(key));
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
                        nearCache.remove(serializeKeys ? key : toObject(key));
                    }
                }
            } else {
                for (Data key : keys) {
                    nearCache.remove(serializeKeys ? key : toObject(key));
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
