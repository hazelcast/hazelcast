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

package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.client.cache.impl.ClientCacheProxy;
import com.hazelcast.client.cache.impl.ClientCacheStatisticsImpl;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.RemoteCallHook;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.nearcache.NearCache.UpdateSemantic.READ_UPDATE;
import static com.hazelcast.internal.nearcache.NearCache.UpdateSemantic.WRITE_UPDATE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * An {@link ICacheInternal} implementation which
 * handles Near Cache specific behaviour of methods.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
@SuppressWarnings("checkstyle:methodcount")
public class NearCachedClientCacheProxy<K, V> extends ClientCacheProxy<K, V> {

    private boolean useObjectKey;
    private boolean useObjectValue;
    private boolean cacheOnUpdate;
    private boolean invalidateOnChange;
    private UUID invalidationListenerId;
    private NearCacheManager nearCacheManager;
    private NearCache<Object, Object> nearCache;

    public NearCachedClientCacheProxy(CacheConfig<K, V> cacheConfig, ClientContext context) {
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
        useObjectKey = !nearCacheConfig.isSerializeKeys();
        useObjectValue = nearCacheConfig.getInMemoryFormat() == InMemoryFormat.OBJECT;

        nearCacheManager = getContext().getNearCacheManager(getServiceName());
        nearCache = nearCacheManager.getOrCreateNearCache(nameWithPrefix, nearCacheConfig);
        CacheStatistics localCacheStatistics = super.getLocalCacheStatistics();
        ((ClientCacheStatisticsImpl) localCacheStatistics).setNearCacheStats(nearCache.getNearCacheStats());

        registerInvalidationListener();
        if (nearCacheConfig.getPreloaderConfig().isEnabled()) {
            nearCacheManager.startPreloading(nearCache, new ICacheDataStructureAdapter<>(this));
        }
    }

    private static NearCacheConfig checkNearCacheConfig(NearCacheConfig nearCacheConfig,
                                                        NativeMemoryConfig nativeMemoryConfig) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat != NATIVE) {
            return nearCacheConfig;
        }

        checkTrue(nativeMemoryConfig.isEnabled(),
                "Enable native memory config to use NATIVE in-memory-format for Near Cache");
        return nearCacheConfig;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V callGetSync(Object key, ExpiryPolicy expiryPolicy) {
        Object nearCacheKey = useObjectKey ? key : toData(key);
        V value = (V) getCachedValue(nearCacheKey, true);
        if (value != NOT_CACHED) {
            return value;
        }

        try {
            Data keyData = toData(nearCacheKey);
            long reservationId = nearCache.tryReserveForUpdate(nearCacheKey, keyData, READ_UPDATE);
            value = super.callGetSync(keyData, expiryPolicy);
            if (reservationId != NOT_RESERVED) {
                value = (V) tryPublishReserved(nearCacheKey, value, reservationId);
            }
            return value;
        } catch (Throwable throwable) {
            invalidateNearCache(nearCacheKey);
            throw rethrow(throwable);
        }
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    protected CompletableFuture<V> callGetAsync(Object key, ExpiryPolicy expiryPolicy,
                                                BiConsumer<V, Throwable> statsCallback) {
        Object nearCacheKey = useObjectKey ? key : toData(key);
        V value = (V) getCachedValue(nearCacheKey, false);
        if (value != NOT_CACHED) {
            return CompletableFuture.completedFuture(value);
        }

        try {
            Data dataKey = toData(nearCacheKey);
            long reservationId = nearCache.tryReserveForUpdate(nearCacheKey, dataKey, READ_UPDATE);
            // if we haven't managed to reserve, no need to create a new consumer.
            BiConsumer<V, Throwable> consumer = reservationId == NOT_RESERVED
                    ? statsCallback
                    : (valueData, throwable) -> {
                try {
                    if (statsCallback != null) {
                        statsCallback.accept(valueData, throwable);
                    }
                } finally {
                    if (throwable != null) {
                        NearCachedClientCacheProxy.this.invalidateNearCache(nearCacheKey);
                    } else {
                        NearCachedClientCacheProxy.this.tryPublishReserved(nearCacheKey, valueData,
                                reservationId, false);
                    }
                }
            };
            return super.callGetAsync(dataKey, expiryPolicy, consumer);
        } catch (Throwable t) {
            invalidateNearCache(nearCacheKey);
            throw rethrow(t);
        }
    }

    @Override
    protected V callPutSync(K key, Data keyData, V value, Data valueData,
                            Data expiryPolicyData, boolean isGet) {
        Supplier<V> remoteCallSupplier = () -> {
            try {
                return NearCachedClientCacheProxy.super.callPutSync(key, keyData,
                        value, valueData, expiryPolicyData, isGet);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        };

        return byUpdatingNearCache(remoteCallSupplier, key, keyData, value, valueData, false);
    }

    @Override
    protected CompletableFuture<V> callPutAsync(K key, Data keyData, V value, Data valueData,
                                                Data expiryPolicyData,
                                                boolean isGet, boolean withCompletionEvent,
                                                BiConsumer<V, Throwable> statsCallback) {
        Supplier<CompletableFuture<V>> remoteCallSupplier
                = () -> NearCachedClientCacheProxy.super.callPutAsync(key, keyData, value, valueData,
                expiryPolicyData, isGet, withCompletionEvent, null);

        return byUpdatingNearCacheAsync(remoteCallSupplier, key, keyData, value, valueData, statsCallback, false);
    }

    @Override
    protected Boolean callPutIfAbsentSync(K key, Data keyData, V value, Data valueData,
                                          Data expiryPolicyData, boolean withCompletionEvent) {
        Supplier<Boolean> remoteCallSupplier = () -> {
            try {
                return NearCachedClientCacheProxy.super.callPutIfAbsentSync(key, keyData,
                        value, valueData, expiryPolicyData, withCompletionEvent);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        };

        return byUpdatingNearCache(remoteCallSupplier, key, keyData, value, valueData, true);
    }

    @Override
    protected CompletableFuture<Boolean> callPutIfAbsentAsync(K key, Data keyData, V value, Data valueData,
                                                              Data expiryPolicyData, boolean withCompletionEvent,
                                                              BiConsumer<Boolean, Throwable> statsCallback) {
        Supplier<CompletableFuture<Boolean>> remoteCallSupplier = ()
                -> NearCachedClientCacheProxy.super.callPutIfAbsentAsync(key, keyData, value, valueData,
                expiryPolicyData, withCompletionEvent, null);

        return byUpdatingNearCacheAsync(remoteCallSupplier, key, keyData, value, valueData, statsCallback, true);
    }

    @Override
    protected boolean callReplaceSync(K key, Data keyData, V newValue, Data newValueData,
                                      Data oldValueData, Data expiryPolicyData) {
        Supplier<Boolean> remoteCallSupplier = () -> {
            try {
                return NearCachedClientCacheProxy.super.callReplaceSync(key, keyData, newValue,
                        newValueData, oldValueData, expiryPolicyData);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        };

        return byUpdatingNearCache(remoteCallSupplier, key, keyData, newValue, newValueData, true);
    }

    @Override
    protected CompletableFuture<Boolean> callReplaceAsync(K key, Data keyData, V newValue,
                                                          Data newValueData, Data oldValueData,
                                                          Data expiryPolicyData, boolean withCompletionEvent,
                                                          BiConsumer<Boolean, Throwable> statsCallback) {
        Supplier<CompletableFuture<Boolean>> remoteCallSupplier = ()
                -> NearCachedClientCacheProxy.super.callReplaceAsync(key, keyData, newValue, newValueData,
                oldValueData, expiryPolicyData, withCompletionEvent, null);

        return byUpdatingNearCacheAsync(remoteCallSupplier, key, keyData, newValue, newValueData, statsCallback, true);
    }

    @Override
    protected V callGetAndReplaceSync(K key, Data keyData, V newValue, Data newValueData,
                                      Data expiryPolicyData, boolean withCompletionEvent) {
        Supplier<V> remoteCallSupplier = () -> {
            try {
                return NearCachedClientCacheProxy.super.callGetAndReplaceSync(key, keyData,
                        newValue, newValueData, expiryPolicyData, withCompletionEvent);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        };

        return byUpdatingNearCache(remoteCallSupplier, key, keyData, newValue, newValueData, false);
    }

    @Override
    protected <T> CompletableFuture<T> callGetAndReplaceAsync(K key, Data keyData,
                                                              V newValue, Data newValueData,
                                                              Data expiryPolicyData,
                                                              boolean withCompletionEvent,
                                                              BiConsumer<T, Throwable> statsCallback) {
        Supplier<CompletableFuture<T>> remoteCallSupplier
                = () -> NearCachedClientCacheProxy.super.callGetAndReplaceAsync(key, keyData, newValue, newValueData,
                expiryPolicyData, withCompletionEvent, null);

        return byUpdatingNearCacheAsync(remoteCallSupplier, key, keyData, newValue, newValueData, statsCallback, false);
    }

    @Override
    protected boolean callRemoveSync(K key, Data keyData, Data oldValueData, boolean withCompletionEvent) {
        try {
            return super.callRemoveSync(key, keyData, oldValueData, withCompletionEvent);
        } finally {
            invalidateNearCache(toNearCacheKey(key, keyData));
        }
    }

    @Override
    protected CompletableFuture<Boolean> callRemoveAsync(K key, Data keyData, Data oldValueData,
                                                         boolean withCompletionEvent,
                                                         BiConsumer<Boolean, Throwable> statsCallback) {
        CompletableFuture<Boolean> future
                = super.callRemoveAsync(key, keyData, oldValueData, withCompletionEvent, null);

        return future.whenCompleteAsync((removed, throwable) -> {
            try {
                if (statsCallback != null) {
                    statsCallback.accept(removed, throwable);
                }
            } finally {
                invalidateNearCache(toNearCacheKey(key, keyData));
            }
        });
    }

    @Nonnull
    @Override
    protected V callGetAndRemoveSync(K key, Data keyData) {
        try {
            return super.callGetAndRemoveSync(key, keyData);
        } finally {
            invalidateNearCache(toNearCacheKey(key, keyData));
        }
    }

    @Nonnull
    @Override
    protected <T> CompletableFuture<T> callGetAndRemoveAsync(K key, Data keyData,
                                                             BiConsumer<T, Throwable> statsCallback) {
        CompletableFuture<T> future = super.callGetAndRemoveAsync(key, keyData, null);
        return future.whenCompleteAsync((t, throwable) -> {
            if (statsCallback != null) {
                statsCallback.accept(t, throwable);
            }
            invalidateNearCache(toNearCacheKey(key, keyData));
        });
    }

    @Override
    protected void getAllInternal(Set<? extends K> keys,
                                  Collection<Data> dataKeys,
                                  ExpiryPolicy expiryPolicy,
                                  List<Object> resultingKeyValuePairs,
                                  long startNanos) {
        if (!useObjectKey) {
            toDataKeysWithReservations(keys, dataKeys, null, null);
        }
        Collection<?> ncKeys = useObjectKey ? new LinkedList<K>(keys) : dataKeys;

        populateResultFromNearCache(ncKeys, resultingKeyValuePairs);
        if (ncKeys.isEmpty()) {
            return;
        }

        Map<Object, Long> reservations = createHashMap(ncKeys.size());
        Map<Data, Object> reverseKeyMap = null;
        if (useObjectKey) {
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

    private void toDataKeysWithReservations(Collection<?> keys, Collection<Data> dataKeys,
                                            Map<Object, Long> reservations,
                                            Map<Data, Object> reverseKeyMap) {
        for (Object key : keys) {
            Data keyData = toData(key);
            if (reservations != null) {
                long reservationId = nearCache.tryReserveForUpdate(key, keyData, READ_UPDATE);
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
            long reservationId = nearCache.tryReserveForUpdate(key, key, READ_UPDATE);
            if (reservationId != NOT_RESERVED) {
                reservations.put(key, reservationId);
            }
        }
    }

    private void populateResultFromRemote(int currentSize, List<Object> resultingKeyValuePairs,
                                          Map<Object, Long> reservations,
                                          Map<Data, Object> reverseKeyMap) {
        for (int i = currentSize; i < resultingKeyValuePairs.size(); i += 2) {
            Data keyData = (Data) resultingKeyValuePairs.get(i);
            Data valueData = (Data) resultingKeyValuePairs.get(i + 1);

            Object ncKey = useObjectKey ? reverseKeyMap.get(keyData) : keyData;
            if (useObjectKey) {
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
        if (!useObjectKey) {
            serializedKeys = new HashSet<>(keys.size());
        }
        super.setExpiryPolicyInternal(keys, expiryPolicy, serializedKeys);
        invalidate(keys, serializedKeys);
    }

    @Override
    protected boolean setExpiryPolicyInternal(K key, ExpiryPolicy expiryPolicy) {
        boolean result = super.setExpiryPolicyInternal(key, expiryPolicy);
        if (useObjectKey) {
            invalidateNearCache(key);
        } else {
            invalidateNearCache(toData(key));
        }
        return result;
    }

    @Override
    protected void callPutAllSync(List<Map.Entry<Data, Data>>[] entriesPerPartition, Data expiryPolicyData,
                                  RemoteCallHook<K, V> nearCachingHook, long startNanos) {
        try {
            super.callPutAllSync(entriesPerPartition, expiryPolicyData, nearCachingHook, startNanos);
            nearCachingHook.onRemoteCallSuccess(null);
        } catch (Throwable t) {
            nearCachingHook.onRemoteCallFailure();
            throw rethrow(t);
        }
    }

    @Override
    protected RemoteCallHook<K, V> createPutAllNearCachingHook(int keySetSize) {
        return cacheOnUpdate
                ? new PutAllCacheOnUpdateHook(keySetSize)
                : new PutAllInvalidateHook(keySetSize);
    }

    /**
     * Populates near cache when configured {@link
     * com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy} is {@link
     * com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE}.
     *  
     * Only used with putAll calls.
     */
    private class PutAllCacheOnUpdateHook implements RemoteCallHook<K, V> {
        // Holds near-cache-key, near-cache-value and reservation-id
        private final List<Object> keyValueId;

        PutAllCacheOnUpdateHook(int keySetSize) {
            keyValueId = new ArrayList<>(keySetSize * 3);
        }

        @Override
        public void beforeRemoteCall(K key, Data keyData, V value, Data valueData) {
            keyValueId.add(toNearCacheKey(key, keyData));
            keyValueId.add(toNearCacheValue(value, valueData));
            keyValueId.add(nearCache.tryReserveForUpdate(toNearCacheKey(key, keyData), keyData, WRITE_UPDATE));
        }

        @Override
        public void onRemoteCallSuccess(Operation remoteCall) {
            for (int i = 0; i < keyValueId.size(); i += 3) {
                Object nearCacheKey = keyValueId.get(i);
                Object nearCacheValue = keyValueId.get(i + 1);
                long reservationId = (long) keyValueId.get(i + 2);

                if (reservationId != NOT_RESERVED) {
                    tryPublishReserved(nearCacheKey, nearCacheValue,
                            reservationId, false);
                } else {
                    // If we got no reservation, we invalidate near cache. This is
                    // because caller node is responsible for near cache invalidations
                    // on its local. No received invalidation event is applied to a near
                    // cache if the invalidation is generated as a result of call which
                    // is instantiated from the same client which has near cache on.
                    invalidateNearCache(nearCacheKey);
                }
            }
        }

        @Override
        public void onRemoteCallFailure() {
            for (int i = 0; i < keyValueId.size(); i += 3) {
                invalidateNearCache(keyValueId.get(i));
            }
        }
    }

    /**
     * Invalidates near cache when configured {@link
     * com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy} is {@link
     * com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#INVALIDATE}.
     *  
     * Only used with putAll calls.
     */
    private class PutAllInvalidateHook implements RemoteCallHook<K, V> {

        private final List<Object> nearCacheKeys;

        PutAllInvalidateHook(int keySetSize) {
            this.nearCacheKeys = new ArrayList<>(keySetSize);
        }

        @Override
        public void beforeRemoteCall(K key, Data keyData, V value, Data valueData) {
            nearCacheKeys.add(toNearCacheKey(key, keyData));
        }

        @Override
        public void onRemoteCallSuccess(@Nullable Operation remoteCall) {
            for (Object nearCacheKey : nearCacheKeys) {
                invalidateNearCache(nearCacheKey);
            }
        }

        @Override
        public void onRemoteCallFailure() {
            onRemoteCallSuccess(null);
        }
    }

    private void invalidate(Set<? extends K> keys, Set<Data> keysData) {
        if (useObjectKey) {
            for (K key : keys) {
                invalidateNearCache(key);
            }
        } else {
            for (Data key : keysData) {
                invalidateNearCache(key);
            }
        }
    }

    @Override
    protected boolean containsKeyInternal(Object key) {
        key = useObjectKey ? key : toData(key);
        Object cached = getCachedValue(key, false);
        if (cached != NOT_CACHED) {
            return cached != null;
        }
        return super.containsKeyInternal(key);
    }

    @Override
    protected void loadAllInternal(Set<? extends K> keys, List<Data> dataKeys,
                                   boolean replaceExistingValues, CompletionListener completionListener) {
        try {
            super.loadAllInternal(keys, dataKeys, replaceExistingValues, completionListener);
        } finally {
            if (useObjectKey) {
                for (K key : keys) {
                    invalidateNearCache(key);
                }
            } else {
                for (Data dataKey : dataKeys) {
                    invalidateNearCache(dataKey);
                }
            }
        }
    }

    @Override
    protected void removeAllKeysInternal(Set<? extends K> keys,
                                         Collection<Data> dataKeys, long startNanos) {
        try {
            super.removeAllKeysInternal(keys, dataKeys, startNanos);
        } finally {
            if (useObjectKey) {
                for (K key : keys) {
                    invalidateNearCache(key);
                }
            } else {
                for (Data dataKey : dataKeys) {
                    invalidateNearCache(dataKey);
                }
            }
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
        key = useObjectKey ? key : toData(key);
        try {
            return super.invokeInternal(key, epData, arguments);
        } finally {
            invalidateNearCache(key);
        }
    }

    @Override
    public void close() {
        try {
            removeInvalidationListener();
            nearCacheManager.clearNearCache(nearCache.getName());
        } finally {
            super.close();
        }
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
        removeInvalidationListener();
        nearCacheManager.destroyNearCache(nearCache.getName());
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

    private void invalidateNearCache(Object key) {
        assert key != null;

        nearCache.invalidate(key);
    }

    /**
     * Publishes value got from remote or deletes
     * reserved record when remote value is {@code null}.
     *
     * @param key                      key to update in Near Cache
     * @param remoteValue              fetched value from server
     * @param reservationId            reservation ID for this key
     * @param deserializeReturnedValue deserialize returned value
     * @return last known value for the key
     */
    private Object tryPublishReserved(Object key, Object remoteValue,
                                      long reservationId, boolean deserializeReturnedValue) {
        assert remoteValue != NOT_CACHED;

        // caching null value is not supported for ICache Near Cache
        if (remoteValue == null) {
            // needed to delete reserved record
            invalidateNearCache(key);
            return null;
        }

        Object cachedValue = null;
        if (reservationId != NOT_RESERVED) {
            cachedValue = nearCache.tryPublishReserved(key, remoteValue,
                    reservationId, deserializeReturnedValue);
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

    UUID addNearCacheInvalidationListener(EventHandler eventHandler) {
        return registerListener(createNearCacheInvalidationListenerCodec(), eventHandler);
    }

    private ListenerMessageCodec createNearCacheInvalidationListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return CacheAddNearCacheInvalidationListenerCodec.encodeRequest(nameWithPrefix, localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return CacheAddNearCacheInvalidationListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return CacheRemoveEntryListenerCodec.encodeRequest(nameWithPrefix, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return CacheRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    private void registerInvalidationListener() {
        if (!invalidateOnChange) {
            return;
        }

        EventHandler eventHandler = new NearCacheInvalidationEventHandler();
        invalidationListenerId = addNearCacheInvalidationListener(eventHandler);
    }

    private void removeInvalidationListener() {
        if (!invalidateOnChange) {
            return;
        }

        UUID registrationId = invalidationListenerId;
        if (registrationId != null) {
            getContext().getRepairingTask(getServiceName()).deregisterHandler(nameWithPrefix);
            getContext().getListenerService().deregisterListener(registrationId);
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
        public void beforeListenerRegister(Connection connection) {
            RepairingTask repairingTask = getContext().getRepairingTask(getServiceName());
            repairingHandler = repairingTask.registerAndGetHandler(nameWithPrefix, nearCache);
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

    /**
     * Called by SYNC methods of {@link NearCachedClientCacheProxy}.
     *
     * This method first calls a remote operation then, depending on the
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy},
     * populates or invalidates local near cache.
     *
     * @param remoteCallSupplier    wraps a synchronous remote call to server.
     * @param key                   the key in object format
     * @param keyData               the key in data format
     * @param value                 the value in object format
     * @param valueData             the value in data format
     * @param calledByBooleanMethod {@code true} if
     *                              this method is called from a method which returns
     *                              boolean response, otherwise set it to {@code false}
     * @param <T>                   result of remote call
     * @return response of remote call
     */
    private <T> T byUpdatingNearCache(Supplier<T> remoteCallSupplier, K key, Data keyData,
                                      V value, Data valueData, boolean calledByBooleanMethod) {
        Object nearCacheKey = toNearCacheKey(key, keyData);
        try {
            long reservationId = cacheOnUpdate
                    ? nearCache.tryReserveForUpdate(nearCacheKey, keyData, WRITE_UPDATE) : NOT_RESERVED;
            T response = remoteCallSupplier.get();
            if (reservationId != NOT_RESERVED
                    && (calledByBooleanMethod && response instanceof Boolean ? ((Boolean) response) : true)) {
                Object nearCacheValue = toNearCacheValue(value, valueData);
                tryPublishReserved(nearCacheKey, nearCacheValue, reservationId, false);
            } else {
                invalidateNearCache(nearCacheKey);
            }
            return response;
        } catch (Throwable t) {
            invalidateNearCache(nearCacheKey);
            throw rethrow(t);
        }
    }

    /**
     * Called by ASYNC methods of {@link NearCachedClientCacheProxy}.
     *
     * This method first calls a remote operation then, depending on the
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy},
     * populates or invalidates local near cache.
     *
     * @param remoteCallSupplier    wraps an asynchronous remote call to server.
     * @param key                   the key in object format
     * @param keyData               the key in data format
     * @param value                 the value in object format
     * @param valueData             the value in data format
     * @param calledByBooleanMethod {@code true} if
     *                              this method is called from a method which returns
     *                              boolean response, otherwise set it to {@code false}
     * @param <T>                   result of remote call
     * @return response of remote call
     */
    private <T> CompletableFuture<T> byUpdatingNearCacheAsync(Supplier<CompletableFuture<T>> remoteCallSupplier,
                                                              K key, Data keyData, V value, Data valueData,
                                                              BiConsumer<T, Throwable> statsCallback,
                                                              boolean calledByBooleanMethod) {
        Object nearCacheKey = toNearCacheKey(key, keyData);
        long reservationId = cacheOnUpdate
                ? nearCache.tryReserveForUpdate(nearCacheKey, keyData, WRITE_UPDATE) : NOT_RESERVED;
        CompletableFuture<T> future = remoteCallSupplier.get();
        if (reservationId != NOT_RESERVED) {
            return future.whenCompleteAsync((response, throwable) -> {
                if (statsCallback != null) {
                    statsCallback.accept(response, throwable);
                }

                if (throwable != null) {
                    invalidateNearCache(nearCacheKey);
                } else if ((calledByBooleanMethod && response instanceof Boolean ? ((Boolean) response) : true)) {
                    Object nearCacheValue = toNearCacheValue(value, valueData);
                    tryPublishReserved(nearCacheKey, nearCacheValue, reservationId, false);
                } else {
                    // Remove reservation, we haven't managed to put value.
                    invalidateNearCache(nearCacheKey);
                }
            });
        } else {
            return future.whenCompleteAsync((response, throwable) -> {
                if (statsCallback != null) {
                    statsCallback.accept(response, throwable);
                }

                invalidateNearCache(nearCacheKey);
            });
        }
    }

    private Object toNearCacheKey(K key, Data keyData) {
        return useObjectKey ? key : keyData;
    }

    private Object toNearCacheValue(V value, Data valueData) {
        return useObjectValue ? value : valueData;
    }
}
