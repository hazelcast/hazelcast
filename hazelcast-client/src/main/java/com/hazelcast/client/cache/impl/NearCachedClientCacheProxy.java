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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

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
        "checkstyle:classfanoutcomplexity", "checkstyle:anoninnerlength", "checkstyle:parameternumber"})
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
    protected V getSyncInternal(K key, Data keyData, ExpiryPolicy expiryPolicy) {
        keyData = serializeKeys ? toData(key) : null;
        V value = (V) getCachedValue(key, keyData, true);
        if (hasCached(value)) {
            return value;
        }

        try {
            long reservationId = tryReserveForUpdate(key, keyData, true);
            value = super.getSyncInternal(key, keyData, expiryPolicy);
            if (reservationId != NOT_RESERVED) {
                value = toObject(tryPublishReserved(key, keyData, value, reservationId, true));
            }
            return value;
        } catch (Throwable throwable) {
            invalidateNearCache(key, keyData);
            throw rethrow(throwable);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected InternalCompletableFuture<V> getAsyncInternal(K key, Data keyData, ExpiryPolicy expiryPolicy,
                                                            ExecutionCallback<V> callback) {
        keyData = serializeKeys ? toData(key) : null;
        V value = (V) getCachedValue(key, keyData, false);
        if (hasCached(value)) {
            return new CompletedFuture<V>(getSerializationService(), value, getContext().getExecutionService().getUserExecutor());
        }

        try {
            long reservationId = tryReserveForUpdate(key, keyData, true);
            GetAsyncCallback wrapped = new GetAsyncCallback(key, keyData, callback, reservationId);
            return super.getAsyncInternal(key, keyData, expiryPolicy, wrapped);
        } catch (Throwable t) {
            invalidateNearCache(key, keyData);
            throw rethrow(t);
        }
    }

    private static boolean hasCached(Object value) {
        return value != NOT_CACHED;
    }


    @Override
    protected V putSyncInternal0(K key, V value, Data keyData, Data valueData,
                                 ExpiryPolicy expiryPolicy, boolean isGet, long startNanos) {
        V response;
        try {
            long reservationId = tryReserveForUpdate(key, keyData, cacheOnUpdate);
            response = super.putSyncInternal0(key, value, keyData, valueData, expiryPolicy, isGet, startNanos);
            cacheOrInvalidate(key, value, keyData, valueData, reservationId);
        } catch (Throwable t) {
            invalidateNearCache(key, keyData);
            throw rethrow(t);
        }
        return response;
    }

    @SuppressWarnings("unchecked")
    private void cacheOrInvalidate(K key, V value, Data keyData, Data valueData, long reservationId) {
        if (cacheOnUpdate && reservationId != NOT_RESERVED) {
            V valueToStore = (V) nearCache.selectToSave(valueData, value);
            tryPublishReserved(key, keyData, valueToStore, reservationId, false);
        } else {
            invalidateNearCache(key, keyData);
        }
    }

    @Override
    protected ClientDelegatingFuture putAsyncInternal0(K key, V value, Data keyData, Data valueData, Data expiryPolicyData,
                                                       boolean isGet, boolean withCompletionEvent,
                                                       OneShotExecutionCallback<V> callback) {
        try {
            long reservationId = tryReserveForUpdate(key, keyData, cacheOnUpdate);
            PutAsyncOneShotCallback wrapper = new PutAsyncOneShotCallback(key, value, keyData,
                    valueData, callback, reservationId);
            return super.putAsyncInternal0(key, value, keyData, valueData, expiryPolicyData,
                    isGet, withCompletionEvent, wrapper);
        } catch (Throwable t) {
            invalidateNearCache(key, keyData);
            throw rethrow(t);
        }
    }

    @Override
    protected Object putIfAbsentSyncInternal0(K key, V value, Data keyData, Data valueData, Data expiryPolicyData,
                                              boolean withCompletionEvent, long startNanos) {
        Object response;
        try {
            long reservationId = tryReserveForUpdate(key, keyData, cacheOnUpdate);
            response = super.putIfAbsentSyncInternal0(key, value, keyData, valueData,
                    expiryPolicyData, withCompletionEvent, startNanos);
            cacheOrInvalidate(key, value, keyData, valueData, reservationId);
        } catch (Throwable t) {
            invalidateNearCache(key, keyData);
            throw rethrow(t);
        }
        return response;
    }

    @Override
    protected ClientDelegatingFuture<Boolean> putIfAbsentAsyncInternal0(K key, V value, Data keyData, Data valueData,
                                                                        Data expiryPolicyData,
                                                                        ExecutionCallback<Boolean> callback,
                                                                        boolean withCompletionEvent, long startNanos) {
        try {
            long reservationId = tryReserveForUpdate(key, keyData, cacheOnUpdate);
            CacheOrInvalidateCallback<Boolean> wrapped
                    = new CacheOrInvalidateCallback<Boolean>(key, value, keyData, valueData, callback, reservationId);
            return super.putIfAbsentAsyncInternal0(key, value, keyData, valueData, expiryPolicyData, wrapped,
                    withCompletionEvent, startNanos);
        } catch (Throwable t) {
            invalidateNearCache(key, keyData);
            throw rethrow(t);
        }
    }

    @Override
    protected Object getAndRemoveSyncInternal0(K key, Data keyData) {
        try {
            return super.getAndRemoveSyncInternal0(key, keyData);
        } finally {
            invalidateNearCache(key, keyData);
        }
    }

    @Override
    protected <T> ICompletableFuture<T> getAndRemoveAsyncInternal0(K key, Data keyData, long startNanos) {
        try {
            return super.getAndRemoveAsyncInternal0(key, keyData, startNanos);
        } finally {
            invalidateNearCache(key, keyData);
        }
    }

    @Override
    protected boolean replaceSyncInternal0(K key, V newValue, Data keyData, Data oldValueData,
                                           Data newValueData, Data expiryPolicyData, long startNanos) {

        boolean replaced;
        try {
            long reservationId = tryReserveForUpdate(key, keyData, cacheOnUpdate);
            replaced = super.replaceSyncInternal0(key, newValue, keyData, oldValueData,
                    newValueData, expiryPolicyData, startNanos);
            if (replaced) {
                cacheOrInvalidate(key, newValue, keyData, newValueData, reservationId);
            } else {
                invalidateNearCache(key, keyData);
            }
        } catch (Throwable t) {
            invalidateNearCache(key, keyData);
            throw rethrow(t);
        }

        return replaced;
    }

    @Override
    protected <T> ICompletableFuture<T> replaceAsyncInternal0(K key, V newValue, Data keyData, Data oldValueData,
                                                              Data newValueData, Data expiryPolicyData,
                                                              ExecutionCallback<T> callback,
                                                              boolean withCompletionEvent, long startNanos) {
        ICompletableFuture<T> future;
        try {
            long reservationId = tryReserveForUpdate(key, keyData, cacheOnUpdate);
            CacheOrInvalidateCallback<T> wrapped = new CacheOrInvalidateCallback<T>(key, newValue, keyData, newValueData,
                    callback, reservationId);
            future = super.replaceAsyncInternal0(key, newValue, keyData, oldValueData, newValueData,
                    expiryPolicyData, wrapped, withCompletionEvent, startNanos);
        } catch (Throwable t) {
            invalidateNearCache(key, keyData);
            throw rethrow(t);
        }
        return future;
    }

    @Override
    protected Object replaceAndGetSyncInternal0(K key, V newValue, Data keyData, Data newValueData, Data expiryPolicyData,
                                                boolean withCompletionEvent, long startNanos) {
        Object response;
        try {
            long reservationId = tryReserveForUpdate(key, keyData, cacheOnUpdate);
            response = super.replaceAndGetSyncInternal0(key, newValue, keyData, newValueData, expiryPolicyData,
                    withCompletionEvent, startNanos);
            cacheOrInvalidate(key, newValue, keyData, newValueData, reservationId);
        } catch (Throwable t) {
            invalidateNearCache(key, keyData);
            throw rethrow(t);
        }
        return response;
    }

    @Override
    protected List<Map.Entry<Data, Data>> getAllInternal(Set<? extends K> keys, Collection<Data> dataKeys,
                                                         ExpiryPolicy expiryPolicy, Map<K, V> resultMap, long startNanos) {
        if (serializeKeys) {
            objectToDataCollection(keys, dataKeys, getSerializationService(), NULL_KEY_IS_NOT_ALLOWED);
        }
        Collection<?> keySet = serializeKeys ? dataKeys : new ArrayList<Object>(keys);
        populateResultFromNearCache(keySet, resultMap);
        if (keySet.isEmpty()) {
            return null;
        }

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
            if (hasCached(cached)) {
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
                    V cachedValue = toObject(tryPublishReserved(key, keyData, remoteValue, reservationId, true));
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
                    V cachedValue = toObject(tryPublishReserved(key, null, value, reservationId, true));
                    V newValue = toObject(cachedValue);
                    resultMap.put(key, newValue);
                    reservations.remove(key);
                }
            }
        }
    }

    @Override
    protected void putToAllPartitionsAndWaitForCompletion(List<Map.Entry<Data, Data>>[] entriesPerPartition,
                                                          ExpiryPolicy expiryPolicy, long startNanos)
            throws ExecutionException, InterruptedException {

        try {
            Map<Object, Long> reservations = cacheOnUpdate ? createReservations(entriesPerPartition) : null;
            super.putToAllPartitionsAndWaitForCompletion(entriesPerPartition, expiryPolicy, startNanos);

            // cache or invalidate Near Cache
            for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
                List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
                if (entries != null) {
                    for (Map.Entry<Data, Data> entry : entries) {
                        K keyObject = ((QuartetMapEntry) entry).getKeyObject();
                        V valueObject = ((QuartetMapEntry) entry).getValueObject();
                        Data keyData = entry.getKey();
                        Object accessKey = serializeKeys ? keyData : keyObject;
                        Long reservationId = reservations == null ? NOT_RESERVED : reservations.remove(accessKey);
                        assert reservationId != null;
                        cacheOrInvalidate(keyObject, valueObject, keyData, entry.getValue(), reservationId.longValue());
                    }
                }
            }
        } catch (Throwable t) {
            for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
                List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
                if (entries != null) {
                    for (Map.Entry<Data, Data> entry : entries) {
                        K keyObject = ((QuartetMapEntry) entry).getKeyObject();
                        invalidateNearCache(keyObject, entry.getKey());
                    }
                }
            }
            throw rethrow(t);
        }
    }

    private Map<Object, Long> createReservations(List<Map.Entry<Data, Data>>[] entriesPerPartition) {
        Map<Object, Long> reservations = new HashMap<Object, Long>();

        for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
            List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
            if (entries != null) {
                for (Map.Entry<Data, Data> entry : entries) {
                    Data keyData = entry.getKey();
                    K keyObject = ((QuartetMapEntry) entry).getKeyObject();
                    Object accessKey = serializeKeys ? keyData : keyObject;
                    reservations.put(accessKey, tryReserveForUpdate(keyObject, keyData, cacheOnUpdate));
                }
            }
        }
        return reservations;
    }

    @Override
    protected boolean containsKeyInternal(Object key) {
        key = serializeKeys ? toData(key) : key;
        Object cached = getCachedValue(key, false);
        if (hasCached(cached)) {
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
    public void onRemoveSyncInternal(K key, Data keyData) {
        try {
            super.onRemoveSyncInternal(key, keyData);
        } finally {
            invalidateNearCache(key, keyData);
        }
    }

    @Override
    protected void onRemoveAsyncInternal(K key, Data keyData, ClientDelegatingFuture future, ExecutionCallback callback) {
        try {
            super.onRemoveAsyncInternal(key, keyData, future, callback);
        } finally {
            invalidateNearCache(key, keyData);
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
    protected Object invokeInternal(K key, Data keyData, Data epData, Object[] arguments) {
        try {
            return super.invokeInternal(key, keyData, epData, arguments);
        } finally {
            invalidateNearCache(key, keyData);
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

    private Object getCachedValue(K key, Data keyData, boolean deserializeValue) {
        Object accessKey = serializeKeys ? keyData : key;
        Object cached = nearCache.get(accessKey);
        // caching null values is not supported for ICache Near Cache
        assert cached != CACHED_AS_NULL;

        if (cached == null) {
            return NOT_CACHED;
        }
        return deserializeValue ? toObject(cached) : cached;
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

    private void invalidateNearCache(K key, Data keyData) {
        assert key != null;

        nearCache.remove(serializeKeys ? keyData : key);
    }

    private void invalidateNearCache(K key) {
        assert key != null;

        nearCache.remove(key);
    }

    private void invalidateNearCache(Data keyData) {
        assert keyData != null;

        nearCache.remove(keyData);
    }

    private long tryReserveForUpdate(K key, Data keyData, boolean enableReservation) {
        if (enableReservation) {
            return nearCache.tryReserveForUpdate(serializeKeys ? keyData : key);
        }

        return NOT_RESERVED;
    }

    private long tryReserveForUpdate(Object key) {
        return nearCache.tryReserveForUpdate(key);
    }

    /**
     * Publishes value got from remote or deletes reserved record when remote value is {@code null}.
     *
     * @param key           key to update in Near Cache
     * @param keyData       key in Data format.
     * @param remoteValue   fetched value from server
     * @param reservationId reservation ID for this key
     * @param deserialize   deserialize returned value
     * @return last known value for the key
     */
    @SuppressWarnings("unchecked")
    private Object tryPublishReserved(K key, Data keyData, Object remoteValue, long reservationId, boolean deserialize) {
        assert hasCached(remoteValue);

        // caching null value is not supported for ICache Near Cache
        if (remoteValue == null) {
            // needed to delete reserved record
            invalidateNearCache(key, keyData);
            return null;
        }

        V cachedValue = null;
        if (reservationId != NOT_RESERVED) {
            Object accessKey = serializeKeys ? keyData : key;
            cachedValue = (V) nearCache.tryPublishReserved(accessKey, remoteValue, reservationId, deserialize);
        }
        return cachedValue == null ? remoteValue : cachedValue;
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

        private final K key;
        private final Data keyData;
        private final ExecutionCallback<V> callback;
        private final long reservationId;

        GetAsyncCallback(K key, Data keyData, ExecutionCallback<V> callback, long reservationId) {
            this.key = key;
            this.keyData = keyData;
            this.callback = callback;
            this.reservationId = reservationId;
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
                invalidateNearCache(key, keyData);
            }
        }
    }

    private final class PutAsyncOneShotCallback extends OneShotExecutionCallback<V> {

        private final long reservationId;
        private final K key;
        private final V newValue;
        private final Data keyData;
        private final Data newValueData;
        private final OneShotExecutionCallback<V> statsCallback;


        public PutAsyncOneShotCallback(K key, V newValue, Data keyData, Data newValueData,
                                       OneShotExecutionCallback<V> callback, long reservationId) {
            this.key = key;
            this.newValue = newValue;
            this.keyData = keyData;
            this.newValueData = newValueData;
            this.statsCallback = callback;
            this.reservationId = reservationId;

        }

        @Override
        protected void onResponseInternal(V response) {
            try {
                if (statsCallback != null) {
                    statsCallback.onResponseInternal(response);
                }
            } finally {
                cacheOrInvalidate(key, newValue, keyData, newValueData, reservationId);
            }
        }

        @Override
        protected void onFailureInternal(Throwable t) {
            try {
                if (statsCallback != null) {
                    statsCallback.onFailureInternal(t);
                }
            } finally {
                invalidateNearCache(key, keyData);
            }
        }
    }

    private final class CacheOrInvalidateCallback<T> implements ExecutionCallback<T> {
        private final K key;
        private final V value;
        private final Data keyData;
        private final Data valueData;
        private final ExecutionCallback<T> callback;
        private final long reservationId;

        CacheOrInvalidateCallback(K key, V value, Data keyData, Data valueData,
                                  ExecutionCallback<T> callback, long reservationId) {
            this.key = key;
            this.value = value;
            this.keyData = keyData;
            this.valueData = valueData;
            this.callback = callback;
            this.reservationId = reservationId;
        }

        @Override
        public void onResponse(T response) {
            try {
                if (callback != null) {
                    callback.onResponse(response);
                }
            } finally {
                cacheOrInvalidate(key, value, keyData, valueData, reservationId);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            try {
                if (callback != null) {
                    callback.onFailure(t);
                }
            } finally {
                invalidateNearCache(key, keyData);
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
