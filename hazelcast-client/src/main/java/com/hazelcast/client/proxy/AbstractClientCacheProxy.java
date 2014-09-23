/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.client.CacheGetAllRequest;
import com.hazelcast.cache.impl.client.CacheGetRequest;
import com.hazelcast.cache.impl.client.CacheSizeRequest;
import com.hazelcast.client.nearcache.ClientNearCache;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientCallFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.DelegatingFuture;

import javax.cache.CacheException;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;

/**
 * Abstract ICache implementation
 */
abstract class AbstractClientCacheProxy<K, V>
        extends AbstractBaseClientCacheProxy<K, V>
        implements ICache<K, V> {

    protected AbstractClientCacheProxy(CacheConfig cacheConfig, ClientCacheDistributedObject delegate) {
        super(cacheConfig, delegate);
    }

    //region ICACHE: JCACHE EXTENSION
    @Override
    public ICompletableFuture<V> getAsync(K key) {
        return getAsync(key, null);
    }

    @Override
    public ICompletableFuture<V> getAsync(K key, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        validateNotNull(key);
        final Data keyData = toData(key);
        Object cached = nearCache != null ? nearCache.get(keyData) : null;
        if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
            return createCompletedFuture(cached);
        }
        CacheGetRequest request = new CacheGetRequest(getDistributedObjectName(), keyData, expiryPolicy);
        ClientCallFuture future;
        final ClientContext context = getContext();
        try {
            future = (ClientCallFuture) context.getInvocationService().invokeOnKeyOwner(request, keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        if (nearCache != null) {
            future.andThenInternal(new ExecutionCallback<Data>() {
                public void onResponse(Data valueData) {
                    storeInNearCache(keyData, valueData, null);
                }

                public void onFailure(Throwable t) {
                }
            });
        }
        return new DelegatingFuture<V>(future, getContext().getSerializationService());
    }

    @Override
    public ICompletableFuture<Void> putAsync(K key, V value) {
        return putAsync(key, value, null);
    }

    @Override
    public ICompletableFuture<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return putAsyncInternal(key, value, expiryPolicy, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return putIfAbsentAsyncInternal(key, value, expiryPolicy, false);
    }

    @Override
    public Future<V> getAndPutAsync(K key, V value) {
        return getAndPutAsync(key, value, null);
    }

    @Override
    public Future<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return putAsyncInternal(key, value, expiryPolicy, true, false);
    }

    @Override
    public ICompletableFuture<Boolean> removeAsync(K key) {
        return removeAsyncInternal(key, null, false, false, false);
    }

    @Override
    public ICompletableFuture<Boolean> removeAsync(K key, V oldValue) {
        return removeAsyncInternal(key, oldValue, true, false, false);
    }

    @Override
    public Future<V> getAndRemoveAsync(K key) {
        return removeAsyncInternal(key, null, false, true, false);
    }

    @Override
    public Future<Boolean> replaceAsync(K key, V value) {
        return replaceAsyncInternal(key, null, value, null, false, false, false);
    }

    @Override
    public Future<Boolean> replaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, null, value, expiryPolicy, false, false, false);
    }

    @Override
    public Future<Boolean> replaceAsync(K key, V oldValue, V newValue) {
        return replaceAsyncInternal(key, oldValue, newValue, null, true, false, false);
    }

    @Override
    public Future<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, oldValue, newValue, expiryPolicy, true, false, false);
    }

    @Override
    public Future<V> getAndReplaceAsync(K key, V value) {
        return replaceAsyncInternal(key, null, value, null, false, true, false);
    }

    @Override
    public Future<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, null, value, expiryPolicy, false, true, false);
    }

    @Override
    public V get(K key, ExpiryPolicy expiryPolicy) {
        final Future<V> f = getAsync(key, expiryPolicy);
        try {
            return f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        validateNotNull(keys);
        if (keys.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
        final Set<Data> keySet = new HashSet(keys.size());
        for (K key : keys) {
            final Data k = toData(key);
            keySet.add(k);
        }
        Map<K, V> result = getAllFromNearCache(keySet);
        if (keySet.isEmpty()) {
            return result;
        }
        final CacheGetAllRequest request = new CacheGetAllRequest(getDistributedObjectName(), keySet, expiryPolicy);
        final MapEntrySet mapEntrySet = toObject(delegate.invoke(request));
        final Set<Map.Entry<Data, Data>> entrySet = mapEntrySet.getEntrySet();
        for (Map.Entry<Data, Data> dataEntry : entrySet) {
            final Data keyData = dataEntry.getKey();
            final Data valueData = dataEntry.getValue();
            final K key = toObject(keyData);
            final V value = toObject(valueData);
            result.put(key, value);
            storeInNearCache(keyData, valueData, value);
        }
        return result;
    }

    private Map<K, V> getAllFromNearCache(Set<Data> keySet) {
        Map<K, V> result = new HashMap<K, V>();
        if (nearCache != null) {
            final Iterator<Data> iterator = keySet.iterator();
            while (iterator.hasNext()) {
                Data key = iterator.next();
                Object cached = nearCache.get(key);
                if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
                    result.put((K) toObject(key), (V) cached);
                    iterator.remove();
                }
            }
        }
        return result;
    }

    @Override
    public void put(K key, V value, ExpiryPolicy expiryPolicy) {
        final ICompletableFuture<Object> f = putAsyncInternal(key, value, expiryPolicy, false, true);
        try {
            f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public V getAndPut(K key, V value, ExpiryPolicy expiryPolicy) {
        final ICompletableFuture<V> f = putAsyncInternal(key, value, expiryPolicy, true, true);
        try {
            return toObject(f.get());
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        validateNotNull(map);
        //TODO implement putAllOperationFactory
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue(), expiryPolicy);
        }
    }

    @Override
    public boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy) {
        final Future<Boolean> f = putIfAbsentAsyncInternal(key, value, expiryPolicy, true);
        try {
            return toObject(f.get());
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        final Future<Boolean> f = replaceAsyncInternal(key, oldValue, newValue, expiryPolicy, true, false, true);
        try {
            return toObject(f.get());
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean replace(K key, V value, ExpiryPolicy expiryPolicy) {
        final Future<Boolean> f = replaceAsyncInternal(key, null, value, expiryPolicy, false, false, true);
        try {
            return toObject(f.get());
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy) {
        final Future<V> f = replaceAsyncInternal(key, null, value, expiryPolicy, false, true, true);
        try {
            return toObject(f.get());
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public int size() {
        ensureOpen();
        try {
            CacheSizeRequest request = new CacheSizeRequest(getDistributedObjectName());
            Integer result = delegate.invoke(request);
            if (result == null) {
                return 0;
            }
            return result;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    @Override
    public CacheStatistics getLocalCacheStatistics() {
        throw new UnsupportedOperationException("local cache Statistics are not implemented yet");
    }

    //endregion ICACHE: JCACHE EXTENSION



}
