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
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.ClientInterceptingDelegatingFuture;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static com.hazelcast.client.cache.impl.ClientCacheProxySupportUtil.checkNearCacheConfig;
import static com.hazelcast.client.cache.impl.ClientCacheProxySupportUtil.createInvalidationListenerCodec;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * An {@link ICacheInternal} implementation which handles Near Cache specific behaviour of methods.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
@SuppressWarnings("checkstyle:methodcount")
public class NearCachedClientCacheProxy<K, V> extends ClientCacheProxy<K, V> {

    private boolean cacheOnUpdate;
    private boolean invalidateOnChange;
    private boolean serializeKeys;

    private NearCacheManager nearCacheManager;
    private NearCache<Object, Object> nearCache;
    private UUID nearCacheMembershipRegistrationId;

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
        cacheOnUpdate = nearCacheConfig.getLocalUpdatePolicy() == CACHE_ON_UPDATE;
        invalidateOnChange = nearCacheConfig.isInvalidateOnChange();
        serializeKeys = nearCacheConfig.isSerializeKeys();

        ICacheDataStructureAdapter<K, V> adapter = new ICacheDataStructureAdapter<>(this);
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
                                                            BiConsumer<V, Throwable> callback) {
        key = serializeKeys ? toData(key) : key;
        V value = (V) getCachedValue(key, false);
        if (value != NOT_CACHED) {
            return InternalCompletableFuture.newCompletedFuture(value);
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
                                              BiConsumer<Boolean, Throwable> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        CacheOrInvalidateCallback<Boolean> wrapped = new CacheOrInvalidateCallback<>(callbackKey, keyData, value,
                valueData, callback);
        super.onPutIfAbsentAsyncInternal(key, value, keyData, valueData, delegatingFuture, wrapped);
    }

    @Override
    protected ClientDelegatingFuture<V> wrapPutAsyncFuture(K key, V value, Data keyData, Data valueData,
                                                           ClientInvocationFuture invocationFuture,
                                                           BiConsumer<V, Throwable> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        PutAsyncOneShotCallback wrapped = new PutAsyncOneShotCallback(callbackKey, keyData, value, valueData, callback);
        ClientDelegatingFuture<V> future = new ClientInterceptingDelegatingFuture<>(invocationFuture,
                getSerializationService(), message -> CachePutCodec.decodeResponse(message).response, wrapped);
        future.whenCompleteAsync(wrapped);
        return future;
    }

    @Override
    protected <T> void onGetAndRemoveAsyncInternal(K key, Data keyData, ClientDelegatingFuture<T> delegatingFuture,
                                                   BiConsumer<T, Throwable> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        InvalidateCallback<T> wrapped = new InvalidateCallback<>(callbackKey, callback);
        super.onGetAndRemoveAsyncInternal(key, keyData, delegatingFuture, wrapped);
    }

    @Override
    protected <T> void onReplaceInternalAsync(K key, V value, Data keyData, Data valueData,
                                              ClientDelegatingFuture<T> delegatingFuture, BiConsumer<T, Throwable> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        CacheOrInvalidateCallback<T> wrapped = new CacheOrInvalidateCallback<>(callbackKey, keyData, value, valueData, callback);
        super.onReplaceInternalAsync(key, value, keyData, valueData, delegatingFuture, wrapped);
    }

    @Override
    protected <T> void onReplaceAndGetAsync(K key, V value, Data keyData, Data valueData,
                                            ClientDelegatingFuture<T> delegatingFuture, BiConsumer<T, Throwable> callback) {
        Object callbackKey = serializeKeys ? keyData : key;
        CacheOrInvalidateCallback<T> wrapped = new CacheOrInvalidateCallback<>(callbackKey, keyData, value, valueData, callback);
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
    public void setExpiryPolicyInternal(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        Set<Data> serializedKeys = null;
        if (serializeKeys) {
            serializedKeys = new HashSet<>(keys.size());
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
    protected void onRemoveAsyncInternal(Object key, Data keyData, ClientDelegatingFuture future,
                                         BiConsumer<Object, Throwable> callback) {
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

    private void cacheOrInvalidate(Object key, Data keyData, V value, Data valueData) {
        if (cacheOnUpdate) {
            nearCache.put(key, keyData, value, valueData);
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
            nearCache.invalidate(key);
        }
    }

    public UUID addNearCacheInvalidationListener(EventHandler eventHandler) {
        return registerListener(createInvalidationListenerCodec(nameWithPrefix), eventHandler);
    }

    private void registerInvalidationListener() {
        if (!invalidateOnChange) {
            return;
        }

        EventHandler eventHandler = new NearCacheInvalidationEventHandler();
        nearCacheMembershipRegistrationId = addNearCacheInvalidationListener(eventHandler);
    }

    private void removeInvalidationListener() {
        if (!invalidateOnChange) {
            return;
        }

        UUID registrationId = nearCacheMembershipRegistrationId;
        if (registrationId != null) {
            getContext().getRepairingTask(getServiceName()).deregisterHandler(nameWithPrefix);
            getContext().getListenerService().deregisterListener(registrationId);
        }
    }

    private final class GetAsyncCallback implements BiConsumer<V, Throwable> {

        private final Object key;
        private final long reservationId;
        private final BiConsumer<V, Throwable> callback;

        GetAsyncCallback(Object key, long reservationId, BiConsumer<V, Throwable> callback) {
            this.key = key;
            this.reservationId = reservationId;
            this.callback = callback;
        }

        @Override
        public void accept(V valueData, Throwable throwable) {
            try {
                if (callback != null) {
                    callback.accept(valueData, throwable);
                }
            } finally {
                if (throwable == null) {
                    tryPublishReserved(key, valueData, reservationId, false);
                } else {
                    invalidateNearCache(key);
                }
            }
        }
    }

    private final class PutAsyncOneShotCallback implements BiConsumer<V, Throwable> {

        private final AtomicBoolean executed;
        private final Object key;
        private final Data keyData;
        private final V newValue;
        private final Data newValueData;
        private final BiConsumer<V, Throwable> statsCallback;

        private PutAsyncOneShotCallback(Object key, Data keyData, V newValue, Data newValueData,
                                        BiConsumer<V, Throwable> callback) {
            this.key = key;
            this.keyData = keyData;
            this.newValue = newValue;
            this.newValueData = newValueData;
            this.statsCallback = callback;
            this.executed = new AtomicBoolean();
        }

        @Override
        public void accept(V v, Throwable throwable) {
            if (!executed.compareAndSet(false, true)) {
                return;
            }
            try {
                if (statsCallback != null) {
                    statsCallback.accept(v, throwable);
                }
            } finally {
                if (throwable == null) {
                    cacheOrInvalidate(key, keyData, newValue, newValueData);
                } else {
                    invalidateNearCache(key);
                }
            }
        }
    }

    private final class CacheOrInvalidateCallback<T> implements BiConsumer<T, Throwable> {

        private final Object key;
        private final Data keyData;
        private final V value;
        private final Data valueData;
        private final BiConsumer<T, Throwable> delegate;

        CacheOrInvalidateCallback(Object key, Data keyData, V value, Data valueData, BiConsumer<T, Throwable> delegate) {
            this.key = key;
            this.keyData = keyData;
            this.value = value;
            this.valueData = valueData;
            this.delegate = delegate;
        }

        @Override
        public void accept(T t, Throwable throwable) {
            try {
                if (delegate != null) {
                    delegate.accept(t, throwable);
                }
            } finally {
                if (throwable == null) {
                    cacheOrInvalidate(key, keyData, value, valueData);
                } else {
                    invalidateNearCache(key);
                }
            }
        }
    }

    private final class InvalidateCallback<T> implements BiConsumer<T, Throwable> {

        private final Object key;
        private final BiConsumer<T, Throwable> delegate;

        InvalidateCallback(Object key, BiConsumer<T, Throwable> delegate) {
            this.key = key;
            this.delegate = delegate;
        }

        @Override
        public void accept(T value, Throwable throwable) {
            try {
                if (delegate != null) {
                    delegate.accept(value, throwable);
                }
            } finally {
                invalidateNearCache(key);
            }
        }
    }

    /**
     * This listener listens server side invalidation events.
     */
    private final class NearCacheInvalidationEventHandler
            extends CacheAddNearCacheInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private volatile RepairingHandler repairingHandler;

        private NearCacheInvalidationEventHandler() {
        }

        @Override
        public void beforeListenerRegister() {
            RepairingTask repairingTask = getContext().getRepairingTask(getServiceName());
            repairingHandler = repairingTask.registerAndGetHandler(nameWithPrefix, nearCache);
        }

        @Override
        public void onListenerRegister() {
        }

        @Override
        public void handleCacheInvalidationEvent(String name, Data key, UUID sourceUuid,
                                                 UUID partitionUuid, long sequence) {
            repairingHandler.handle(key, sourceUuid, partitionUuid, sequence);
        }

        @Override
        public void handleCacheBatchInvalidationEvent(String name, Collection<Data> keys,
                                                      Collection<UUID> sourceUuids,
                                                      Collection<UUID> partitionUuids,
                                                      Collection<Long> sequences) {
            repairingHandler.handle(keys, sourceUuids, partitionUuids, sequences);
        }
    }
}
