/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.MapGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MapRemoveEntryListenerCodec;
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
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
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.Collections.emptyMap;

/**
 * A Client-side {@code IMap} implementation which is fronted by a Near Cache.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class NearCachedClientMapProxy<K, V> extends ClientMapProxy<K, V> {

    private boolean invalidateOnChange;
    private NearCache<Object, Object> nearCache;
    private RepairingHandler repairingHandler;

    private volatile String invalidationListenerId;

    public NearCachedClientMapProxy(String serviceName, String name) {
        super(serviceName, name);
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        ClientContext context = getContext();
        NearCacheConfig nearCacheConfig = context.getClientConfig().getNearCacheConfig(name);
        NearCacheManager nearCacheManager = context.getNearCacheManager();
        IMapDataStructureAdapter<K, V> adapter = new IMapDataStructureAdapter<K, V>(this);
        nearCache = nearCacheManager.getOrCreateNearCache(name, nearCacheConfig, adapter);
        invalidateOnChange = nearCache.isInvalidatedOnChange();

        if (invalidateOnChange) {
            repairingHandler = context.getRepairingTask(SERVICE_NAME).registerAndGetHandler(name, nearCache);
            addNearCacheInvalidationListener(new ClientMapAddNearCacheEventHandler());
        }
    }

    @Override
    protected boolean containsKeyInternal(Data keyData) {
        Object cached = getCachedValue(keyData);
        if (cached != NOT_CACHED) {
            return true;
        }

        return super.containsKeyInternal(keyData);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V getInternal(Data key) {
        Object value = getCachedValue(key);
        if (value != NOT_CACHED) {
            return (V) value;
        }

        if (nearCache.tryReserveForUpdate(key)) {
            value = updateAndGetCachedValue(key, super.getInternal(key));
        } else {
            value = super.getInternal(key);
        }

        return (V) value;
    }

    private Object updateAndGetCachedValue(Data key, Object value) {
        Object cachedValue;
        try {
            nearCache.updateReserved(key, value);
        } finally {
            cachedValue = nearCache.publishReserved(key);
        }

        return cachedValue != null ? cachedValue : value;
    }

    private Object getCachedValue(Data key) {
        Object value = nearCache.get(key);

        if (value == null) {
            return NOT_CACHED;
        }

        if (value == CACHED_AS_NULL) {
            return null;
        }

        return toObject(value);
    }

    @Override
    public ICompletableFuture<V> getAsyncInternal(final Data key) {
        Object value = getCachedValue(key);
        if (value != NOT_CACHED) {
            return new CompletedFuture<V>(getContext().getSerializationService(),
                    value, getContext().getExecutionService().getAsyncExecutor());
        }

        final boolean reserved = nearCache.tryReserveForUpdate(key);
        ICompletableFuture<V> future;
        try {
            future = super.getAsyncInternal(key);
        } catch (Throwable t) {
            if (reserved) {
                nearCache.publishReserved(key);
            }

            throw rethrow(t);
        }

        ((ClientDelegatingFuture) future).andThenInternal(new ExecutionCallback<Data>() {
            @Override
            public void onResponse(Data value) {
                if (reserved) {
                    try {
                        nearCache.updateReserved(key, value);
                    } finally {
                        nearCache.publishReserved(key);
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                if (reserved) {
                    nearCache.publishReserved(key);
                }
            }
        });

        return future;
    }

    @Override
    protected MapRemoveCodec.ResponseParameters removeInternal(Data keyData) {
        MapRemoveCodec.ResponseParameters responseParameters = super.removeInternal(keyData);
        invalidateNearCache(keyData);
        return responseParameters;
    }

    @Override
    protected boolean removeInternal(Data keyData, Data valueData) {
        boolean removed = super.removeInternal(keyData, valueData);
        invalidateNearCache(keyData);
        return removed;
    }

    @Override
    protected void removeAllInternal(Predicate predicate) {
        super.removeAllInternal(predicate);
        nearCache.clear();
    }

    @Override
    protected void deleteInternal(Data keyData) {
        super.deleteInternal(keyData);
        invalidateNearCache(keyData);
    }

    @Override
    protected ICompletableFuture<V> putAsyncInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        ICompletableFuture<V> future = super.putAsyncInternal(ttl, timeunit, keyData, valueData);
        invalidateNearCache(keyData);
        return future;
    }

    @Override
    protected ICompletableFuture<Void> setAsyncInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        ICompletableFuture<Void> future = super.setAsyncInternal(ttl, timeunit, keyData, valueData);
        invalidateNearCache(keyData);
        return future;
    }

    @Override
    protected ICompletableFuture<V> removeAsyncInternal(Data keyData) {
        ICompletableFuture<V> future = super.removeAsyncInternal(keyData);
        invalidateNearCache(keyData);
        return future;
    }

    @Override
    protected boolean tryRemoveInternal(long timeout, TimeUnit timeunit, Data keyData) {
        boolean removed = super.tryRemoveInternal(timeout, timeunit, keyData);
        invalidateNearCache(keyData);
        return removed;
    }

    @Override
    protected boolean tryPutInternal(long timeout, TimeUnit timeunit, Data keyData, Data valueData) {
        boolean putInternal = super.tryPutInternal(timeout, timeunit, keyData, valueData);
        invalidateNearCache(keyData);
        return putInternal;
    }

    @Override
    protected V putInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        V previousValue = super.putInternal(ttl, timeunit, keyData, valueData);
        invalidateNearCache(keyData);
        return previousValue;
    }

    @Override
    protected void putTransientInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        super.putTransientInternal(ttl, timeunit, keyData, valueData);
        invalidateNearCache(keyData);
    }

    @Override
    protected V putIfAbsentInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        V previousValue = super.putIfAbsentInternal(ttl, timeunit, keyData, valueData);
        invalidateNearCache(keyData);
        return previousValue;
    }

    @Override
    protected boolean replaceIfSameInternal(Data keyData, Data oldValueData, Data newValueData) {
        boolean replaceIfSame = super.replaceIfSameInternal(keyData, oldValueData, newValueData);
        invalidateNearCache(keyData);
        return replaceIfSame;
    }

    @Override
    protected V replaceInternal(Data keyData, Data valueData) {
        V v = super.replaceInternal(keyData, valueData);
        invalidateNearCache(keyData);
        return v;
    }

    @Override
    protected void setInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        super.setInternal(ttl, timeunit, keyData, valueData);
        invalidateNearCache(keyData);
    }

    @Override
    protected boolean evictInternal(Data keyData) {
        boolean evicted = super.evictInternal(keyData);
        invalidateNearCache(keyData);
        return evicted;
    }

    @Override
    public void evictAll() {
        super.evictAll();
        nearCache.clear();
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
        super.loadAllInternal(replaceExistingValues, dataKeys);
        invalidateNearCache(dataKeys);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<MapGetAllCodec.ResponseParameters> getAllInternal(Map<Integer, List<Data>> pIdToKeyData, Map<K, V> result) {
        Map<Data, Boolean> reservedKeys = new HashMap<Data, Boolean>();
        List<MapGetAllCodec.ResponseParameters> responses;
        try {

            for (Entry<Integer, List<Data>> partitionKeyEntry : pIdToKeyData.entrySet()) {
                List<Data> keyList = partitionKeyEntry.getValue();
                Iterator<Data> iterator = keyList.iterator();
                while (iterator.hasNext()) {
                    Data key = iterator.next();
                    Object value = getCachedValue(key);
                    if (value != NOT_CACHED) {
                        result.put((K) toObject(key), (V) toObject(value));
                        iterator.remove();
                    } else {
                        reservedKeys.put(key, nearCache.tryReserveForUpdate(key));
                    }
                }
            }

            responses = super.getAllInternal(pIdToKeyData, result);

            for (MapGetAllCodec.ResponseParameters resultParameters : responses) {
                for (Entry<Data, Data> entry : resultParameters.response) {
                    Data key = entry.getKey();
                    if (reservedKeys.get(key)) {
                        Object cachedValue = updateAndGetCachedValue(key, entry.getValue());
                        result.put((K) toObject(key), (V) toObject(cachedValue));
                        reservedKeys.remove(key);
                    }
                }
            }
        } finally {
            releaseReservedKeys(reservedKeys);
        }
        return responses;
    }

    private void releaseReservedKeys(Map<Data, Boolean> reservedKeys) {
        for (Entry<Data, Boolean> entry : reservedKeys.entrySet()) {
            if (entry.getValue()) {
                nearCache.publishReserved(entry.getKey());
            }
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
        Object response = super.executeOnKeyInternal(keyData, entryProcessor);
        invalidateNearCache(keyData);
        return response;
    }

    @Override
    public ICompletableFuture submitToKeyInternal(Data keyData, EntryProcessor entryProcessor) {
        ICompletableFuture future = super.submitToKeyInternal(keyData, entryProcessor);
        invalidateNearCache(keyData);
        return future;
    }

    @Override
    public void submitToKeyInternal(Data keyData, EntryProcessor entryProcessor, ExecutionCallback callback) {
        super.submitToKeyInternal(keyData, entryProcessor, callback);
        invalidateNearCache(keyData);
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
        super.putAllInternal(entryMap);

        for (List<Entry<Data, Data>> entries : entryMap.values()) {
            for (Entry<Data, Data> entry : entries) {
                invalidateNearCache(entry.getKey());
            }
        }
    }

    @Override
    public void clear() {
        super.clear();
        nearCache.clear();
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

    public NearCache<Object, Object> getNearCache() {
        return nearCache;
    }

    private void invalidateNearCache(Data key) {
        nearCache.requestRemoveForReserved(key);
    }

    private void invalidateNearCache(Collection<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        for (Data key : keys) {
            nearCache.requestRemoveForReserved(key);
        }
    }

    public void addNearCacheInvalidationListener(EventHandler handler) {
        try {
            invalidationListenerId = registerListener(createNearCacheEntryListenerCodec(), handler);
        } catch (Exception e) {
            ILogger logger = getContext().getLoggingService().getLogger(getClass());
            logger.severe("-----------------\n Near Cache is not initialized!!! \n-----------------", e);
        }
    }

    private ListenerMessageCodec createNearCacheEntryListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MapAddNearCacheEntryListenerCodec.encodeRequest(name, INVALIDATION.getType(), localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
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

    private final class ClientMapAddNearCacheEventHandler extends MapAddNearCacheEntryListenerCodec.AbstractEventHandler
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
            repairingHandler.handle(key, sourceUuid, partitionUuid, sequence);
        }

        @Override
        public void handle(Collection<Data> keys, Collection<String> sourceUuids,
                           Collection<UUID> partitionUuids, Collection<Long> sequences) {
            repairingHandler.handle(keys, sourceUuids, partitionUuids, sequences);
        }

    }

    // used in tests.
    public ClientContext getClientContext() {
        return getContext();
    }
}
