/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.executor.CompletedFuture;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
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
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:anoninnerlength"})
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
            Data keyData = toData(key);
            long reservationId = nearCache.tryReserveForUpdate(key, keyData);
            value = super.getSyncInternal(keyData, expiryPolicy);
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
    @SuppressWarnings("unchecked")
    protected InternalCompletableFuture<V> getAsyncInternal(Object key, ExpiryPolicy expiryPolicy,
                                                            ExecutionCallback<V> callback) {
        key = serializeKeys ? toData(key) : key;
        V value = (V) getCachedValue(key, false);
        if (value != NOT_CACHED) {
            return new CompletedFuture<V>(getSerializationService(), value, getContext().getExecutionService().getUserExecutor());
        }

        try {
            Data keyData = toData(key);
            long reservationId = nearCache.tryReserveForUpdate(key, keyData);
            GetAsyncCallback getAsyncCallback = new GetAsyncCallback(key, reservationId, callback);
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
    protected void getAllInternal(Set<? extends K> keys, Collection<Data> dataKeys, ExpiryPolicy expiryPolicy,
                                  List<Object> resultingKeyValuePairs, long startNanos) {
        if (serializeKeys) {
            toDataKeysWithReservations(keys, dataKeys, null, null);
        }
        Collection<?> ncKeys = serializeKeys ? dataKeys : new LinkedList<K>(keys);

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
            super.getAllInternal(keys, dataKeys, expiryPolicy, resultingKeyValuePairs, startNanos);
            populateResultFromRemote(currentSize, resultingKeyValuePairs, reservations, reverseKeyMap);
        } finally {
            releaseRemainingReservedKeys(reservations);
        }
    }

    private void toDataKeysWithReservations(Collection<?> keys, Collection<Data> dataKeys, Map<Object, Long> reservations,
                                            Map<Data, Object> reverseKeyMap) {
        for (Object key : keys) {
            Data keyData = toData(key);
            if (reservations != null) {
                long reservationId = tryReserveForUpdate(key, keyData);
                if (reservationId != NOT_RESERVED) {
                    reservations.put(key, reservationId);
                }
            }
            if (reverseKeyMap != null) {
                reverseKeyMap.put(keyData, key);
            }
            dataKeys.add(keyData);
        }
    }

    private void populateResultFromNearCache(Collection<?> keys, List<Object> resultingKeyValuePairs) {
        Iterator<?> iterator = keys.iterator();
        while (iterator.hasNext()) {
            Object key = iterator.next();
            Object cached = getCachedValue(key, true);
            if (cached != NOT_CACHED) {
                resultingKeyValuePairs.add(key);
                resultingKeyValuePairs.add(cached);
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
    protected boolean containsKeyInternal(Object key) {
        key = serializeKeys ? toData(key) : key;
        Object cached = getCachedValue(key, false);
        if (cached != NOT_CACHED) {
            return cached != null;
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
    protected void postDestroy() {
        try {
            removeInvalidationListener();
            nearCacheManager.destroyNearCache(nearCache.getName());
        } finally {
            super.postDestroy();
        }
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
    private void cacheOrInvalidate(Object key, Data keyData, V value, Data valueData) {
        if (cacheOnUpdate) {
            V valueToStore = (V) nearCache.selectToSave(valueData, value);
            nearCache.put(key, keyData, valueToStore);
        } else {
            invalidateNearCache(key);
        }
    }

    private void invalidateNearCache(Object key) {
        assert key != null;

        nearCache.invalidate(key);
    }

    private long tryReserveForUpdate(Object key, Data keyData) {
        return nearCache.tryReserveForUpdate(key, keyData);
    }

    /**
     * Publishes value got from remote or deletes reserved record when remote value is {@code null}.
     *
     * @param key           key to update in Near Cache
     * @param remoteValue   fetched value from server
     * @param reservationId reservation ID for this key
     * @param deserialize   deserialize returned value
     * @return last known value for the key
     */
    private Object tryPublishReserved(Object key, Object remoteValue, long reservationId, boolean deserialize) {
        assert remoteValue != NOT_CACHED;

        // caching null value is not supported for ICache Near Cache
        if (remoteValue == null) {
            // needed to delete reserved record
            invalidateNearCache(key);
            return null;
        }

        Object cachedValue = null;
        if (reservationId != NOT_RESERVED) {
            cachedValue = nearCache.tryPublishReserved(key, remoteValue, reservationId, deserialize);
        }
        return cachedValue == null ? remoteValue : cachedValue;
    }

    private Object tryPublishReserved(Object key, Object remoteValue, long reservationId) {
        return tryPublishReserved(key, remoteValue, reservationId, true);
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

        EventHandler eventHandler = new ConnectedServerVersionAwareNearCacheEventHandler();
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

    private final class GetAsyncCallback implements ExecutionCallback<V> {

        private final Object key;
        private final long reservationId;
        private final ExecutionCallback<V> callback;

        GetAsyncCallback(Object key, long reservationId, ExecutionCallback<V> callback) {
            this.key = key;
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
                tryPublishReserved(key, valueData, reservationId, false);
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
     * Deals with client compatibility.
     * <p>
     * Eventual consistency for Near Cache can be used with server versions >= 3.8,
     * other connected server versions must use {@link Pre38NearCacheEventHandler}.
     */
    private final class ConnectedServerVersionAwareNearCacheEventHandler implements EventHandler<ClientMessage> {

        private final Pre38NearCacheEventHandler pre38EventHandler = new Pre38NearCacheEventHandler();
        private final RepairableNearCacheEventHandler repairingEventHandler = new RepairableNearCacheEventHandler();

        private volatile boolean supportsRepairableNearCache;

        @Override
        public void beforeListenerRegister() {
            repairingEventHandler.beforeListenerRegister();

            supportsRepairableNearCache = supportsRepairableNearCache();

            if (!supportsRepairableNearCache) {
                pre38EventHandler.beforeListenerRegister();

                logger.warning(format("Near Cache for '%s' cache is started in legacy mode", name));
            }
        }

        @Override
        public void onListenerRegister() {
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
            if (supportsRepairableNearCache()) {
                RepairingTask repairingTask = getContext().getRepairingTask(getServiceName());
                repairingHandler = repairingTask.registerAndGetHandler(nameWithPrefix, nearCache);
            } else {
                RepairingTask repairingTask = getContext().getRepairingTask(getServiceName());
                repairingTask.deregisterHandler(nameWithPrefix);
            }
        }

        @Override
        public void onListenerRegister() {
            // NOP
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
                nearCache.invalidate(serializeKeys ? key : toObject(key));
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
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
            nearCache.clear();
        }
    }
}
