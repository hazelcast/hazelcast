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

package com.hazelcast.map.impl.proxy;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.nearcache.KeyStateMarker;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.map.impl.nearcache.StaleReadPreventerNearCacheWrapper;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.nearcache.NearCache.NULL_OBJECT;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * A server-side {@code IMap} implementation which is fronted by a near-cache.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class NearCachedMapProxyImpl<K, V> extends MapProxyImpl<K, V> {

    protected NearCache<Data, Object> nearCache;
    protected KeyStateMarker keyStateMarker;
    protected boolean cacheLocalEntries;

    public NearCachedMapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine, MapConfig mapConfig) {
        super(name, mapService, nodeEngine, mapConfig);
    }

    @Override
    public void initialize() {
        super.initialize();

        init();
    }

    @SuppressWarnings("unchecked")
    protected void init() {
        NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        nearCache = nearCacheProvider.getOrCreateNearCache(name);
        keyStateMarker = getKeyStateMarker();
        cacheLocalEntries = getMapConfig().getNearCacheConfig().isCacheLocalEntries();
    }

    // this operation returns the object in data format,
    // except when it is retrieved from near-cache and near-cache memory format is object
    @Override
    protected Object getInternal(Data key) {
        Object value = getCachedValue(key);
        if (value != null) {
            if (isCachedNull(value)) {
                return null;
            }
            return value;
        }

        boolean marked = keyStateMarker.tryMark(key);

        value = super.getInternal(key);

        if (marked) {
            tryToPutNearCache(key, value);
        }

        return value;
    }

    private void tryToPutNearCache(Data key, Object value) {
        try {
            if (!isOwn(key) || cacheLocalEntries) {
                nearCache.put(key, value);
            }
        } finally {
            if (!keyStateMarker.tryUnmark(key)) {
                invalidateCache(key);
                keyStateMarker.forceUnmark(key);
            }
        }
    }

    @Override
    protected ICompletableFuture<Data> getAsyncInternal(final Data key) {
        Object value = nearCache.get(key);
        if (value != null) {
            if (isCachedNull(value)) {
                value = null;
            }
            return new CompletedFuture<Data>(
                    getNodeEngine().getSerializationService(),
                    value,
                    getNodeEngine().getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR));
        }

        final boolean marked = keyStateMarker.tryMark(key);

        ICompletableFuture<Data> future = super.getAsyncInternal(key);
        future.andThen(new ExecutionCallback<Data>() {
            @Override
            public void onResponse(Data response) {
                if (marked) {
                    tryToPutNearCache(key, response);
                }
            }

            @Override
            public void onFailure(Throwable t) {
            }
        });
        return future;
    }

    protected boolean isCachedNull(Object value) {
        return NULL_OBJECT.equals(value);
    }

    @Override
    protected Data putInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        Data data = super.putInternal(key, value, ttl, timeunit);
        invalidateCache(key);
        return data;
    }

    @Override
    protected boolean tryPutInternal(Data key, Data value, long timeout, TimeUnit timeunit) {
        boolean putInternal = super.tryPutInternal(key, value, timeout, timeunit);
        invalidateCache(key);
        return putInternal;
    }

    @Override
    protected Data putIfAbsentInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        Data data = super.putIfAbsentInternal(key, value, ttl, timeunit);
        invalidateCache(key);
        return data;
    }

    @Override
    protected void putTransientInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        invalidateCache(key);
        super.putTransientInternal(key, value, ttl, timeunit);
    }

    @Override
    protected ICompletableFuture<Data> putAsyncInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        ICompletableFuture<Data> future = super.putAsyncInternal(key, value, ttl, timeunit);
        invalidateCache(key);
        return future;
    }

    @Override
    protected ICompletableFuture<Data> setAsyncInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        ICompletableFuture<Data> future = super.setAsyncInternal(key, value, ttl, timeunit);
        invalidateCache(key);
        return future;
    }

    @Override
    protected boolean replaceInternal(Data key, Data expect, Data update) {
        boolean replaceInternal = super.replaceInternal(key, expect, update);
        invalidateCache(key);
        return replaceInternal;
    }

    @Override
    protected Data replaceInternal(Data key, Data value) {
        Data replaceInternal = super.replaceInternal(key, value);
        invalidateCache(key);
        return replaceInternal;
    }

    @Override
    protected void setInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        super.setInternal(key, value, ttl, timeunit);
        invalidateCache(key);
    }

    @Override
    protected boolean evictInternal(Data key) {
        boolean evictInternal = super.evictInternal(key);
        invalidateCache(key);
        return evictInternal;
    }

    @Override
    protected void evictAllInternal() {
        super.evictAllInternal();
        nearCache.clear();
    }

    @Override
    public void clearInternal() {
        super.clearInternal();
        nearCache.clear();
    }

    @Override
    public void loadAllInternal(boolean replaceExistingValues) {
        super.loadAllInternal(replaceExistingValues);
        if (replaceExistingValues) {
            nearCache.clear();
        }
    }

    @Override
    protected void loadInternal(Iterable<Data> keys, boolean replaceExistingValues) {
        super.loadInternal(keys, replaceExistingValues);
        invalidateCache(keys);
    }

    @Override
    protected Data removeInternal(Data key) {
        Data data = super.removeInternal(key);
        invalidateCache(key);
        return data;
    }

    @Override
    protected void deleteInternal(Data key) {
        super.deleteInternal(key);
        invalidateCache(key);
    }

    @Override
    protected boolean removeInternal(Data key, Data value) {
        boolean removeInternal = super.removeInternal(key, value);
        invalidateCache(key);
        return removeInternal;
    }

    @Override
    protected boolean tryRemoveInternal(Data key, long timeout, TimeUnit timeunit) {
        boolean removeInternal = super.tryRemoveInternal(key, timeout, timeunit);
        invalidateCache(key);
        return removeInternal;
    }

    @Override
    protected ICompletableFuture<Data> removeAsyncInternal(Data key) {
        invalidateCache(key);
        return super.removeAsyncInternal(key);
    }

    @Override
    protected boolean containsKeyInternal(Data keyData) {
        Object cached = nearCache.get(keyData);
        if (cached != null) {
            return !isCachedNull(cached);
        } else {
            return super.containsKeyInternal(keyData);
        }
    }

    @Override
    protected void getAllObjectInternal(List<Data> keys, List<Object> resultingKeyValuePairs) {
        getCachedValue(keys, resultingKeyValuePairs);

        Map<Data, Boolean> keyStates = createHashMap(keys.size());
        for (Data key : keys) {
            keyStates.put(key, keyStateMarker.tryMark(key));
        }


        int currentSize = resultingKeyValuePairs.size();
        super.getAllObjectInternal(keys, resultingKeyValuePairs);

        // only add elements which are not in near-putCache
        for (int i = currentSize; i < resultingKeyValuePairs.size(); ) {
            Data key = toData(resultingKeyValuePairs.get(i++));
            Data value = toData(resultingKeyValuePairs.get(i++));

            boolean marked = keyStates.get(key);
            if (marked) {
                tryToPutNearCache(key, value);
            }
        }
    }

    @Override
    protected void invokePutAllOperationFactory(Address address, long size, int[] partitions, MapEntries[] entries)
            throws Exception {
        super.invokePutAllOperationFactory(address, size, partitions, entries);
        for (MapEntries mapEntries : entries) {
            for (int i = 0; i < mapEntries.size(); i++) {
                invalidateCache(mapEntries.getKey(i));
            }
        }
    }

    @Override
    public Data executeOnKeyInternal(Data key, EntryProcessor entryProcessor) {
        Data data = super.executeOnKeyInternal(key, entryProcessor);
        invalidateCache(key);
        return data;
    }

    @Override
    public Map executeOnKeysInternal(Set<Data> keys, EntryProcessor entryProcessor) {
        Map map = super.executeOnKeysInternal(keys, entryProcessor);
        invalidateCache(keys);
        return map;
    }

    @Override
    public ICompletableFuture executeOnKeyInternal(Data key, EntryProcessor entryProcessor, ExecutionCallback<Object> callback) {
        ICompletableFuture future = super.executeOnKeyInternal(key, entryProcessor, callback);
        invalidateCache(key);
        return future;
    }

    @Override
    public void executeOnEntriesInternal(EntryProcessor entryProcessor, Predicate predicate, List<Data> resultingKeyValuePairs) {
        super.executeOnEntriesInternal(entryProcessor, predicate, resultingKeyValuePairs);

        for (int i = 0; i < resultingKeyValuePairs.size(); i += 2) {
            Data key = resultingKeyValuePairs.get(i);
            invalidateCache(key);
        }
    }

    protected Object getCachedValue(Data key) {
        Object cached = nearCache.get(key);
        if (cached == null) {
            return null;
        }
        mapServiceContext.interceptAfterGet(name, cached);
        return cached;
    }

    protected void getCachedValue(List<Data> keys, List<Object> resultingKeyValuePairs) {
        Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            Data key = iterator.next();
            Object value = getCachedValue(key);
            if (value == null) {
                continue;
            }
            if (!isCachedNull(value)) {
                resultingKeyValuePairs.add(toObject(key));
                resultingKeyValuePairs.add(toObject(value));
            }
            iterator.remove();
        }
    }

    protected void invalidateCache(Data key) {
        if (key != null) {
            nearCache.remove(key);
        }
    }

    protected void invalidateCache(Collection<Data> keys) {
        for (Data key : keys) {
            nearCache.remove(key);
        }
    }

    protected void invalidateCache(Iterable<Data> keys) {
        for (Data key : keys) {
            nearCache.remove(key);
        }
    }

    protected boolean isOwn(Data key) {
        int partitionId = partitionService.getPartitionId(key);
        return partitionService.getPartitionOwner(partitionId).equals(thisAddress);
    }

    public NearCache getNearCache() {
        return nearCache;
    }

    public KeyStateMarker getKeyStateMarker() {
        return ((StaleReadPreventerNearCacheWrapper) nearCache).getKeyStateMarker();
    }
}
