/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.map.impl.nearcache.ClientHeapNearCache;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.client.MapAddNearCacheEntryListenerRequest;
import com.hazelcast.map.impl.client.MapRemoveEntryListenerRequest;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.PortableEntryEvent;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.nearcache.NearCache.NULL_OBJECT;
import static com.hazelcast.util.MapUtil.createHashMap;
import static java.util.Collections.emptyMap;

/**
 * A Client-side {@code IMap} implementation which is fronted by a near-cache.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class NearCachedClientMapProxy<K, V> extends ClientMapProxy<K, V> {

    protected NearCache<Data, Object> nearCache;
    protected volatile String invalidationListenerId;

    public NearCachedClientMapProxy(String serviceName, String name) {
        super(serviceName, name);
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        init();
    }

    protected void init() {
        ClientConfig clientConfig = getContext().getClientConfig();
        NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig(name);
        nearCache = new ClientHeapNearCache<Data>(name, getContext(), nearCacheConfig);

        if (nearCache.isInvalidateOnChange()) {
            addNearCacheInvalidateListener();
        }
    }

    @Override
    protected boolean containsKeyInternal(Data keyData) {
        Object cached = nearCache.get(keyData);
        if (cached != null) {
            return NULL_OBJECT != cached;
        } else {
            return super.containsKeyInternal(keyData);
        }

    }

    @Override
    protected V getInternal(Data keyData) {
        Object cached = nearCache.get(keyData);
        if (cached != null) {
            if (NULL_OBJECT == cached) {
                return null;
            }
            return (V) cached;
        } else {
            V response = super.getInternal(keyData);

            nearCache.put(keyData, response);

            return response;
        }
    }

    @Override
    protected V removeInternal(Data keyData) {
        invalidateNearCache(keyData);
        return super.removeInternal(keyData);
    }

    @Override
    protected boolean removeInternal(Data keyData, Data valueData) {
        invalidateNearCache(keyData);
        return super.removeInternal(keyData, valueData);
    }

    @Override
    protected void deleteInternal(Data keyData) {
        invalidateNearCache(keyData);
        super.deleteInternal(keyData);
    }

    @Override
    public ICompletableFuture<V> getAsyncInternal(final Data keyData) {
        Object cached = nearCache.get(keyData);
        if (cached != null && NULL_OBJECT != cached) {
            return new CompletedFuture<V>(getContext().getSerializationService(),
                    cached, getContext().getExecutionService().getAsyncExecutor());
        }

        ICompletableFuture<V> future = super.getAsyncInternal(keyData);
        future.andThen(new ExecutionCallback<V>() {
            @Override
            public void onResponse(V response) {
                nearCache.put(keyData, response);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        });
        return future;
    }

    @Override
    protected Future<V> putAsyncInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        invalidateNearCache(keyData);
        return super.putAsyncInternal(ttl, timeunit, keyData, valueData);
    }

    @Override
    protected Future<V> removeAsyncInternal(Data keyData) {
        invalidateNearCache(keyData);
        return super.removeAsyncInternal(keyData);
    }

    @Override
    protected Boolean tryRemoveInternal(long timeout, TimeUnit timeunit, Data keyData) {
        invalidateNearCache(keyData);
        return super.tryRemoveInternal(timeout, timeunit, keyData);
    }

    @Override
    protected Boolean tryPutInternal(long timeout, TimeUnit timeunit, Data keyData, Data valueData) {
        invalidateNearCache(keyData);
        return super.tryPutInternal(timeout, timeunit, keyData, valueData);
    }

    @Override
    protected V putInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        invalidateNearCache(keyData);
        return super.putInternal(ttl, timeunit, keyData, valueData);
    }

    @Override
    protected void putTransientInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        invalidateNearCache(keyData);
        super.putTransientInternal(ttl, timeunit, keyData, valueData);
    }

    @Override
    protected V putIfAbsentInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        invalidateNearCache(keyData);
        return super.putIfAbsentInternal(ttl, timeunit, keyData, valueData);
    }

    @Override
    protected Boolean replaceIfSameInternal(Data keyData, Data oldValueData, Data newValueData) {
        invalidateNearCache(keyData);
        return super.replaceIfSameInternal(keyData, oldValueData, newValueData);
    }

    @Override
    protected V replaceInternal(Data keyData, Data valueData) {
        invalidateNearCache(keyData);
        return super.replaceInternal(keyData, valueData);
    }

    @Override
    protected void setInternal(long ttl, TimeUnit timeunit, Data keyData, Data valueData) {
        invalidateNearCache(keyData);
        super.setInternal(ttl, timeunit, keyData, valueData);
    }

    @Override
    protected Boolean evictInternal(Data keyData) {
        invalidateNearCache(keyData);
        return super.evictInternal(keyData);
    }

    @Override
    public void evictAll() {
        nearCache.clear();
        super.evictAll();
    }

    @Override
    public void loadAll(boolean replaceExistingValues) {
        if (replaceExistingValues) {
            nearCache.clear();
        }
        super.loadAll(replaceExistingValues);
    }

    @Override
    protected void loadAllInternal(boolean replaceExistingValues, List<Data> dataKeys) {
        invalidateNearCache(dataKeys);
        super.loadAllInternal(replaceExistingValues, dataKeys);
    }

    @Override
    protected List<MapEntries> getAllInternal(Map<Integer, List<Data>> partitionToKeyData, Map<K, V> result) {
        List<Integer> partitionsWithCachedEntries = new ArrayList<Integer>(partitionToKeyData.size());
        for (Entry<Integer, List<Data>> partitionKeyEntry : partitionToKeyData.entrySet()) {
            List<Data> keyList = partitionKeyEntry.getValue();
            Iterator<Data> iterator = keyList.iterator();
            while (iterator.hasNext()) {
                Data key = iterator.next();
                Object cached = nearCache.get(key);
                if (cached != null && NULL_OBJECT != cached) {
                    result.put((K) toObject(key), (V) cached);
                    iterator.remove();
                }
            }
            if (keyList.isEmpty()) {
                partitionsWithCachedEntries.add(partitionKeyEntry.getKey());
            }
        }

        for (Integer partitionId : partitionsWithCachedEntries) {
            partitionToKeyData.remove(partitionId);
        }

        List<MapEntries> responses = super.getAllInternal(partitionToKeyData, result);
        for (MapEntries entries : responses) {
            for (Entry<Data, Data> entry : entries.entries()) {
                nearCache.put(entry.getKey(), entry.getValue());
            }
        }
        //TODO: This returned value is not be used.
        return responses;
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        LocalMapStats localMapStats = super.getLocalMapStats();
        NearCacheStats nearCacheStats = nearCache.getNearCacheStats();
        ((LocalMapStatsImpl) localMapStats).setNearCacheStats(((NearCacheStatsImpl) nearCacheStats));
        return localMapStats;
    }

    @Override
    protected Map<K, Object> prepareResult(MapEntries mapEntries) {
        if (mapEntries.isEmpty()) {
            return emptyMap();
        }

        Map<K, Object> result = createHashMap(mapEntries.size());
        for (Entry<Data, Data> dataEntry : mapEntries) {
            Data keyData = dataEntry.getKey();
            invalidateNearCache(keyData);
            Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            result.put(key, toObject(valueData));
        }
        return result;
    }

    @Override
    protected void putAllInternal(List<Future<?>> futures, MapEntries[] entriesPerPartition) {
        for (int i = 0; i < entriesPerPartition.length; i++) {
            MapEntries mapEntries = entriesPerPartition[i];
            Collection<Entry<Data, Data>> entries = mapEntries.entries();
            for (Entry<Data, Data> entry : entries) {
                Data keyData = entry.getKey();
                invalidateNearCache(keyData);
            }
        }
        super.putAllInternal(futures, entriesPerPartition);
    }

    @Override
    public void clear() {
        nearCache.clear();
        super.clear();
    }

    @Override
    protected void onDestroy() {
        removeNearCacheInvalidationListener();
        nearCache.destroy();

        super.onDestroy();
    }

    @Override
    protected void onShutdown() {
        removeNearCacheInvalidationListener();
        nearCache.destroy();

        super.onShutdown();
    }

    protected void invalidateNearCache(Data key) {
        nearCache.remove(key);
    }


    protected void invalidateNearCache(Collection<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        for (Data key : keys) {
            nearCache.remove(key);
        }
    }

    protected void addNearCacheInvalidateListener() {
        try {
            MapAddNearCacheEntryListenerRequest addRequest = new MapAddNearCacheEntryListenerRequest(name, false);
            EventHandler handler = new InvalidationListener(this.nearCache);
            BaseClientRemoveListenerRequest removeRequest = new MapRemoveEntryListenerRequest(name);

            invalidationListenerId = registerListener(addRequest, removeRequest, handler);
        } catch (Exception e) {
            Logger.getLogger(ClientHeapNearCache.class).severe(
                    "-----------------\n Near Cache is not initialized!!! \n-----------------", e);
        }
    }


    protected static final class InvalidationListener implements EventHandler<PortableEntryEvent> {

        protected final NearCache nearCache;

        protected InvalidationListener(NearCache nearCache) {
            this.nearCache = nearCache;
        }

        @Override
        public void handle(PortableEntryEvent event) {
            switch (event.getEventType()) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case MERGED:
                case EVICTED:
                case EXPIRED:
                    final Data key = event.getKey();
                    nearCache.remove(key);
                    break;
                case CLEAR_ALL:
                case EVICT_ALL:
                    nearCache.clear();
                    break;
                default:
                    throw new IllegalArgumentException("Not a known event type " + event.getEventType());
            }
        }

        @Override
        public void beforeListenerRegister() {
            nearCache.clear();
        }

        @Override
        public void onListenerRegister() {
            nearCache.clear();
        }
    }

    protected void removeNearCacheInvalidationListener() {
        String invalidationListenerId = this.invalidationListenerId;
        if (invalidationListenerId == null) {
            return;
        }

        deregisterListener(invalidationListenerId);
    }
}
