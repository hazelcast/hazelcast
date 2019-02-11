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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
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
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.internal.nearcache.impl.NearCacheDataFetcher;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.executor.DelegatingFuture;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.instance.BuildInfo.calculateVersion;
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
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:anoninnerlength"})
public class NearCachedClientCacheProxy<K, V> extends ClientCacheProxy<K, V> implements NearCacheDataFetcher<V> {

    // eventually consistent Near Cache can only be used with server versions >= 3.8
    private final int minConsistentNearCacheSupportingServerVersion = calculateVersion("3.8");

    private boolean cacheOnUpdate;
    private boolean invalidateOnChange;
    private boolean serializeKeys;

    private NearCacheManager nearCacheManager;
    private NearCache<K, V> nearCache;
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
        nearCache.unwrap(DefaultNearCache.class).setNearCacheDataFetcher(this);

        CacheStatistics localCacheStatistics = super.getLocalCacheStatistics();
        ((ClientCacheStatisticsImpl) localCacheStatistics).setNearCacheStats(nearCache.getNearCacheStats());

        registerInvalidationListener();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V getSyncInternal(Object key, ExpiryPolicy expiryPolicy) {
        return nearCache.get((K) key, expiryPolicy);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ICompletableFuture<V> getAsyncInternal(Object key, ExpiryPolicy expiryPolicy,
                                                     ExecutionCallback<V> statsCallback) {
        InternalCompletableFuture completableFuture
                = (InternalCompletableFuture) nearCache.getAsync((K) key, expiryPolicy);
        DelegatingFuture<V> future
                = new DelegatingFuture<V>(completableFuture, getSerializationService());
        if (statsCallback != null) {
            future.andThen(statsCallback);
        }
        return future;
    }

    @Override
    protected void getAllInternal(Set<? extends K> keys,
                                  ExpiryPolicy expiryPolicy,
                                  Map result, long startNanos) {
        nearCache.getAll((Collection<K>) keys, result, startNanos);
    }

    @Override
    protected boolean containsKeyInternal(Object key) {
        return nearCache.containsKey((K) key);
    }

    @Override
    protected void onPutSyncInternal(K key, V value, Data keyData, Data valueData) {
        try {
            super.onPutSyncInternal(key, value, keyData, valueData);
        } finally {
            cacheOrInvalidate(serializeKeys ? keyData : key, keyData, value, valueData);
        }
    }

    @Override
    protected void onPutIfAbsentSyncInternal(K key, V value, Data keyData, Data valueData) {
        try {
            super.onPutIfAbsentSyncInternal(key, value, keyData, valueData);
        } finally {
            cacheOrInvalidate(serializeKeys ? keyData : key, keyData, value, valueData);
        }
    }

    @Override
    protected void onPutIfAbsentAsyncInternal(K key, V value, Data keyData, Data valueData,
                                              ClientDelegatingFuture<Boolean> delegatingFuture,
                                              ExecutionCallback<Boolean> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        CacheOrInvalidateCallback<Boolean> wrapped = new CacheOrInvalidateCallback<Boolean>(callbackKey, keyData, value,
                valueData, callback);
        super.onPutIfAbsentAsyncInternal(key, value, keyData, valueData, delegatingFuture, wrapped);
    }

    @Override
    protected ClientDelegatingFuture<V> wrapPutAsyncFuture(K key, V value, Data keyData, Data valueData,
                                                           ClientInvocationFuture invocationFuture,
                                                           OneShotExecutionCallback<V> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        PutAsyncOneShotCallback wrapped = new PutAsyncOneShotCallback(callbackKey, keyData, value, valueData, callback);
        return super.wrapPutAsyncFuture(key, value, keyData, valueData, invocationFuture, wrapped);
    }

    @Override
    protected <T> void onGetAndRemoveAsyncInternal(K key, Data keyData, ClientDelegatingFuture<T> delegatingFuture,
                                                   ExecutionCallback<T> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        InvalidateCallback<T> wrapped = new InvalidateCallback<T>(callbackKey, callback);
        super.onGetAndRemoveAsyncInternal(key, keyData, delegatingFuture, wrapped);
    }

    @Override
    protected <T> void onReplaceInternalAsync(K key, V value, Data keyData, Data valueData,
                                              ClientDelegatingFuture<T> delegatingFuture, ExecutionCallback<T> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        CacheOrInvalidateCallback<T> wrapped = new CacheOrInvalidateCallback<T>(callbackKey, keyData, value, valueData, callback);
        super.onReplaceInternalAsync(key, value, keyData, valueData, delegatingFuture, wrapped);
    }

    @Override
    protected <T> void onReplaceAndGetAsync(K key, V value, Data keyData, Data valueData,
                                            ClientDelegatingFuture<T> delegatingFuture, ExecutionCallback<T> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        CacheOrInvalidateCallback<T> wrapped = new CacheOrInvalidateCallback<T>(callbackKey, keyData, value, valueData, callback);
        super.onReplaceAndGetAsync(key, value, keyData, valueData, delegatingFuture, wrapped);
    }

    @Override
    public void setExpiryPolicyInternal(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        Set<Data> serializedKeys = null;
        if (serializeKeys) {
            serializedKeys = new HashSet<Data>(keys.size());
        }
        super.setExpiryPolicyInternal(keys, expiryPolicy, serializedKeys);
        invalidate(keys, serializedKeys);
    }

    @Override
    protected boolean setExpiryPolicyInternal(K key, ExpiryPolicy expiryPolicy) {
        boolean result = super.setExpiryPolicyInternal(key, expiryPolicy);
        if (serializeKeys) {
            invalidateNearCache(toData(key));
        } else {
            invalidateNearCache(key);
        }
        return result;
    }

    @Override
    protected void putAllInternal(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy, Map<Object, Data> keyMap,
                                  List<Map.Entry<Data, Data>>[] entriesPerPartition, long startNanos) {
        try {
            if (!serializeKeys) {
                keyMap = createHashMap(map.size());
            }
            super.putAllInternal(map, expiryPolicy, keyMap, entriesPerPartition, startNanos);
            cacheOrInvalidate(map, keyMap, entriesPerPartition, true);
        } catch (Throwable t) {
            cacheOrInvalidate(map, keyMap, entriesPerPartition, false);
            throw rethrow(t);
        }
    }

    private void invalidate(Set<? extends K> keys, Set<Data> keysData) {
        if (serializeKeys) {
            for (Data key : keysData) {
                invalidateNearCache(key);
            }
        } else {
            for (K key : keys) {
                invalidateNearCache(key);
            }
        }
    }

    private void cacheOrInvalidate(Map<? extends K, ? extends V> map, Map<Object, Data> keyMap,
                                   List<Map.Entry<Data, Data>>[] entriesPerPartition, boolean isCacheOrInvalidate) {
        if (serializeKeys) {
            for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
                List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
                if (entries != null) {
                    for (Map.Entry<Data, Data> entry : entries) {
                        Data key = entry.getKey();
                        if (isCacheOrInvalidate) {
                            // FIXME: the null value produces an unwanted deserialization if Near Cache in-memory format is OBJECT
                            cacheOrInvalidate(key, key, null, entry.getValue());
                        } else {
                            invalidateNearCache(key);
                        }
                    }
                }
            }
        } else {
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                K key = entry.getKey();
                if (isCacheOrInvalidate) {
                    cacheOrInvalidate(key, keyMap.get(key), entry.getValue(), null);
                } else {
                    invalidateNearCache(key);
                }
            }
        }
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
    protected void postDestroy() {
        try {
            removeInvalidationListener();
            nearCacheManager.destroyNearCache(nearCache.getName());
        } finally {
            super.postDestroy();
        }
    }

    @SuppressWarnings("unchecked")
    private void cacheOrInvalidate(Object key, Data keyData, V value, Data valueData) {
        if (cacheOnUpdate) {
            // TODO
            return;
        }
        nearCache.invalidate(key);

    }

    private void invalidateNearCache(Object key) {
        assert key != null;

        nearCache.invalidate(key);
    }

    public String addNearCacheInvalidationListener(EventHandler eventHandler) {
        return registerListener(createInvalidationListenerCodec(), eventHandler);
    }

    private void registerInvalidationListener() {
        if (!invalidateOnChange) {
            return;
        }

        EventHandler eventHandler = new NearCacheInvalidationEventHandler();
        nearCacheMembershipRegistrationId = addNearCacheInvalidationListener(eventHandler);
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

    private boolean supportsRepairableNearCache() {
        return getConnectedServerVersion() >= minConsistentNearCacheSupportingServerVersion;
    }

    private void removeInvalidationListener() {
        if (!invalidateOnChange) {
            return;
        }

        String registrationId = nearCacheMembershipRegistrationId;
        if (registrationId != null) {
            getContext().getRepairingTask(getServiceName()).deregisterHandler(nameWithPrefix);
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

    @Override
    public InternalCompletableFuture<Boolean> invokeContainsKeyOperation(Object key) {
        ClientInvocationFuture invocationFuture = super.invokeContainsKey(key);
        return new ClientDelegatingFuture(invocationFuture, getSerializationService(), CACHE_CONTAINS_KEY_DECODER);
    }

    @Override
    public InternalCompletableFuture invokeGetOperation(Data keyData, Data dataParam,
                                                        boolean asyncGet, long startTimeNanos) {
        return super.invokeCacheGet(keyData, dataParam, false);
    }

    @Override
    public InternalCompletableFuture invokeGetAllOperation(Collection<Data> dataKeys,
                                                           Data dataParam, long startTimeNanos) {
        return new ClientDelegatingFuture(super.invokeGetAll(dataKeys, dataParam),
                getSerializationService(), CACHE_GET_ALL_RESPONSE_DECODER);
    }

    @Override
    public V getWithoutNearCaching(Object key, Object param, long startTimeNanos) {
        return (V) toObject(super.getSyncInternal(key, (ExpiryPolicy) param));
    }

    @Override
    public ICompletableFuture getAsyncWithoutNearCaching(Object key, Object param, long startTimeNanos) {
        return super.getAsyncInternal(key, (ExpiryPolicy) param, null);
    }

    @Override
    public boolean isStatsEnabled() {
        return statisticsEnabled;
    }

    @Override
    public void interceptAfterGet(String name, V val) {
        // NOP
    }

    private final class PutAsyncOneShotCallback extends OneShotExecutionCallback<V> {

        private final Object key;
        private final Data keyData;
        private final V newValue;
        private final Data newValueData;
        private final OneShotExecutionCallback<V> statsCallback;

        private PutAsyncOneShotCallback(Object key, Data keyData, V newValue, Data newValueData,
                                        OneShotExecutionCallback<V> callback) {
            this.key = key;
            this.keyData = keyData;
            this.newValue = newValue;
            this.newValueData = newValueData;
            this.statsCallback = callback;
        }

        @Override
        protected void onResponseInternal(V response) {
            try {
                if (statsCallback != null) {
                    statsCallback.onResponseInternal(response);
                }
            } finally {
                cacheOrInvalidate(key, keyData, newValue, newValueData);
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
        private final V value;
        private final Data valueData;
        private final ExecutionCallback<T> callback;

        CacheOrInvalidateCallback(Object key, Data keyData, V value, Data valueData, ExecutionCallback<T> callback) {
            this.key = key;
            this.keyData = keyData;
            this.value = value;
            this.valueData = valueData;
            this.callback = callback;
        }

        @Override
        public void onResponse(T response) {
            try {
                if (callback != null) {
                    callback.onResponse(response);
                }
            } finally {
                cacheOrInvalidate(key, keyData, value, valueData);
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

    private final class InvalidateCallback<T> implements ExecutionCallback<T> {

        private final Object key;
        private final ExecutionCallback<T> callback;

        InvalidateCallback(Object key, ExecutionCallback<T> callback) {
            this.key = key;
            this.callback = callback;
        }

        @Override
        public void onResponse(T response) {
            try {
                if (callback != null) {
                    callback.onResponse(response);
                }
            } finally {
                invalidateNearCache(key);
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
     * Eventual consistency for Near Cache can be used with server versions >= 3.8
     * For repairing functionality please see {@link RepairingHandler}
     * handleCacheInvalidationEventV14 and handleCacheBatchInvalidationEventV14
     *
     * If server version is < 3.8 and client version is >= 3.8, eventual consistency is not supported
     * Following methods handle the old behaviour:
     * handleCacheBatchInvalidationEventV10 and handleCacheInvalidationEventV10
     */
    private final class NearCacheInvalidationEventHandler
            extends CacheAddInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private String clientUuid;
        private volatile RepairingHandler repairingHandler;
        private volatile boolean supportsRepairableNearCache;

        private NearCacheInvalidationEventHandler() {
            this.clientUuid = getContext().getClusterService().getLocalClient().getUuid();
        }

        @Override
        public void beforeListenerRegister() {
            supportsRepairableNearCache = supportsRepairableNearCache();

            if (supportsRepairableNearCache) {
                RepairingTask repairingTask = getContext().getRepairingTask(getServiceName());
                repairingHandler = repairingTask.registerAndGetHandler(nameWithPrefix, nearCache);
            } else {
                RepairingTask repairingTask = getContext().getRepairingTask(getServiceName());
                repairingTask.deregisterHandler(nameWithPrefix);
                logger.warning(format("Near Cache for '%s' cache is started in legacy mode", name));
            }
        }

        @Override
        public void onListenerRegister() {
            if (!supportsRepairableNearCache) {
                nearCache.clear();
            }
        }

        @Override
        public void handleCacheInvalidationEventV10(String name, Data key, String sourceUuid) {
            if (clientUuid.equals(sourceUuid)) {
                return;
            }
            if (key != null) {
                nearCache.invalidate(serializeKeys ? key : toObject(key));
            } else {
                nearCache.clear();
            }
        }

        @Override
        public void handleCacheInvalidationEventV14(String name, Data key, String sourceUuid,
                                                    UUID partitionUuid, long sequence) {
            repairingHandler.handle(key, sourceUuid, partitionUuid, sequence);
        }

        @Override
        public void handleCacheBatchInvalidationEventV10(String name, Collection<Data> keys,
                                                         Collection<String> sourceUuids) {
            if (sourceUuids != null && !sourceUuids.isEmpty()) {
                Iterator<Data> keysIt = keys.iterator();
                Iterator<String> sourceUuidsIt = sourceUuids.iterator();
                while (keysIt.hasNext() && sourceUuidsIt.hasNext()) {
                    Data key = keysIt.next();
                    String sourceUuid = sourceUuidsIt.next();
                    if (!clientUuid.equals(sourceUuid)) {
                        nearCache.invalidate(serializeKeys ? key : toObject(key));
                    }
                }
            } else {
                for (Data key : keys) {
                    nearCache.invalidate(serializeKeys ? key : toObject(key));
                }
            }
        }

        @Override
        public void handleCacheBatchInvalidationEventV14(String name, Collection<Data> keys,
                                                         Collection<String> sourceUuids,
                                                         Collection<UUID> partitionUuids,
                                                         Collection<Long> sequences) {
            repairingHandler.handle(keys, sourceUuids, partitionUuids, sequences);
        }
    }
}
