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

package com.hazelcast.map.impl.proxy;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.nearcache.NearCache.NULL_OBJECT;

/**
 * A server-side {@code IMap} implementation which is fronted by a near-cache.
 *
 * @param <K> the key type for this {@code IMap} proxy.
 * @param <V> the value type for this {@code IMap} proxy.
 */
public class NearCachedMapProxyImpl<K, V> extends MapProxyImpl<K, V> {

    protected NearCache nearCache;
    protected boolean cacheLocalEntries;

    public NearCachedMapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine) {
        super(name, mapService, nodeEngine);
    }

    @Override
    public void initialize() {
        super.initialize();

        init();
    }

    protected void init() {
        NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        this.nearCache = nearCacheProvider.getOrCreateNearCache(name);
        this.cacheLocalEntries = getMapConfig().getNearCacheConfig().isCacheLocalEntries();
    }

    // this operation returns the object in data format except
    // it is got from near-cache and near-cache memory format is object.
    @Override
    protected Object getInternal(Data key) {
        Object value = getCachedValue(key);
        if (value != null) {
            if (isCachedNull(value)) {
                return null;
            }
            return value;
        }

        value = super.getInternal(key);

        if (!isOwn(key) || cacheLocalEntries) {
            nearCache.put(key, toData(value));
        }

        return value;
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


        ICompletableFuture<Data> future = super.getAsyncInternal(key);
        future.andThen(new ExecutionCallback<Data>() {
            @Override
            public void onResponse(Data response) {
                if (!isOwn(key) || cacheLocalEntries) {
                    nearCache.put(key, response);
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
        invalidateCache(key);
        return super.putInternal(key, value, ttl, timeunit);
    }

    @Override
    protected boolean tryPutInternal(Data key, Data value, long timeout, TimeUnit timeunit) {
        invalidateCache(key);
        return super.tryPutInternal(key, value, timeout, timeunit);
    }

    @Override
    protected Data putIfAbsentInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        invalidateCache(key);
        return super.putIfAbsentInternal(key, value, ttl, timeunit);
    }

    @Override
    protected void putTransientInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        super.putTransientInternal(key, value, ttl, timeunit);
        invalidateCache(key);
    }

    @Override
    protected ICompletableFuture<Data> putAsyncInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        invalidateCache(key);
        return super.putAsyncInternal(key, value, ttl, timeunit);
    }

    @Override
    protected boolean replaceInternal(Data key, Data expect, Data update) {
        invalidateCache(key);
        return super.replaceInternal(key, expect, update);
    }

    @Override
    protected Data replaceInternal(Data key, Data value) {
        invalidateCache(key);
        return super.replaceInternal(key, value);
    }

    @Override
    protected void setInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        invalidateCache(key);
        super.setInternal(key, value, ttl, timeunit);
    }

    @Override
    protected boolean evictInternal(Data key) {
        invalidateCache(key);
        return super.evictInternal(key);
    }

    @Override
    protected void evictAllInternal() {
        nearCache.clear();
        super.evictAllInternal();
    }

    @Override
    public void loadAllInternal(boolean replaceExistingValues) {
        if (replaceExistingValues) {
            nearCache.clear();
        }
        super.loadAllInternal(replaceExistingValues);
    }

    @Override
    protected void loadInternal(Iterable keys, boolean replaceExistingValues) {
        invalidateCache(keys);
        super.loadInternal(keys, replaceExistingValues);
    }

    @Override
    protected Data removeInternal(Data key) {
        invalidateCache(key);
        return super.removeInternal(key);
    }

    @Override
    protected void deleteInternal(Data key) {
        invalidateCache(key);
        super.deleteInternal(key);
    }

    @Override
    protected boolean removeInternal(Data key, Data value) {
        invalidateCache(key);
        return super.removeInternal(key, value);
    }

    @Override
    protected boolean tryRemoveInternal(Data key, long timeout, TimeUnit timeunit) {
        invalidateCache(key);
        return super.tryRemoveInternal(key, timeout, timeunit);
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
    protected void getAllObjectInternal(List<Data> keys, List resultingKeyValuePairs) {
        getCachedValue(keys, resultingKeyValuePairs);

        int currentSize = resultingKeyValuePairs.size();
        super.getAllObjectInternal(keys, resultingKeyValuePairs);

        // only add elements which are not in near-putCache.
        for (int i = currentSize; i < resultingKeyValuePairs.size(); ) {
            Data key = toData(resultingKeyValuePairs.get(i++));
            Data value = toData(resultingKeyValuePairs.get(i++));

            if (!isOwn(key) || cacheLocalEntries) {
                nearCache.put(key, value);
            }
        }
    }

    @Override
    protected Future createPutAllOperationFuture(String name, MapEntries mapEntries, int partitionId) {
        Collection<Entry<Data, Data>> collection = mapEntries.entries();
        for (Entry<Data, Data> entry : collection) {
            invalidateCache(entry.getKey());
        }
        return super.createPutAllOperationFuture(name, mapEntries, partitionId);
    }

    @Override
    public Data executeOnKeyInternal(Data key, EntryProcessor entryProcessor) {
        invalidateCache(key);
        return super.executeOnKeyInternal(key, entryProcessor);
    }

    @Override
    public Map executeOnKeysInternal(Set<Data> keys, EntryProcessor entryProcessor) {
        invalidateCache(keys);
        return super.executeOnKeysInternal(keys, entryProcessor);
    }

    @Override
    public ICompletableFuture executeOnKeyInternal(Data key, EntryProcessor entryProcessor, ExecutionCallback callback) {
        invalidateCache(key);
        return super.executeOnKeyInternal(key, entryProcessor, callback);
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
        NearCache nearCache = this.nearCache;
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
}
