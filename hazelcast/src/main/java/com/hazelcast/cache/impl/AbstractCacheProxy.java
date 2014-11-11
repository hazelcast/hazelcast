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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.CacheException;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;

/**
 * <p>Hazelcast provides extension functionality to default spec interface {@link javax.cache.Cache}.
 * {@link com.hazelcast.cache.ICache} is the designated interface.</p>
 * <p>AbstractCacheProxyExtension provides implementation of various {@link com.hazelcast.cache.ICache} methods.</p>
 * <p>Note: this partial implementation is used by server or embedded mode cache.</p>
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see com.hazelcast.cache.impl.CacheProxy
 * @see com.hazelcast.cache.ICache
 */
abstract class AbstractCacheProxy<K, V>
        extends AbstractInternalCacheProxy<K, V> {

    protected AbstractCacheProxy(CacheConfig cacheConfig, NodeEngine nodeEngine, ICacheService cacheService) {
        super(cacheConfig, nodeEngine, cacheService);
    }

    //region ICACHE: JCACHE EXTENSION
    @Override
    public InternalCompletableFuture<V> getAsync(K key) {
        return getAsync(key, null);
    }

    @Override
    public InternalCompletableFuture<V> getAsync(K key, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        validateNotNull(key);
        final Data keyData = serializationService.toData(key);
        final Operation op = operationProvider.createGetOperation(keyData, expiryPolicy);
        return invoke(op, keyData, false);
    }

    @Override
    public InternalCompletableFuture<Void> putAsync(K key, V value) {
        return putAsync(key, value, null);
    }

    @Override
    public InternalCompletableFuture<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return putAsyncInternal(key, value, expiryPolicy, false, false);
    }

    @Override
    public InternalCompletableFuture<Boolean> putIfAbsentAsync(K key, V value) {
        return putIfAbsentAsyncInternal(key, value, null, false);
    }

    @Override
    public InternalCompletableFuture<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return putIfAbsentAsyncInternal(key, value, expiryPolicy, false);
    }

    @Override
    public ICompletableFuture<V> getAndPutAsync(K key, V value) {
        return getAndPutAsync(key, value, null);
    }

    @Override
    public ICompletableFuture<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return putAsyncInternal(key, value, expiryPolicy, true, false);
    }

    @Override
    public InternalCompletableFuture<Boolean> removeAsync(K key) {
        return removeAsyncInternal(key, null, false, false, false);
    }

    @Override
    public InternalCompletableFuture<Boolean> removeAsync(K key, V oldValue) {
        return removeAsyncInternal(key, oldValue, true, false, false);
    }

    @Override
    public ICompletableFuture<V> getAndRemoveAsync(K key) {
        return removeAsyncInternal(key, null, false, true, false);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V value) {
        return replaceAsyncInternal(key, null, value, null, false, false, false);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, null, value, expiryPolicy, false, false, false);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
        return replaceAsyncInternal(key, oldValue, newValue, null, true, false, false);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, oldValue, newValue, expiryPolicy, true, false, false);
    }

    @Override
    public ICompletableFuture<V> getAndReplaceAsync(K key, V value) {
        return replaceAsyncInternal(key, null, value, null, false, true, false);
    }

    @Override
    public ICompletableFuture<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
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
        final Set<Data> ks = new HashSet(keys.size());
        for (K key : keys) {
            final Data k = serializationService.toData(key);
            ks.add(k);
        }
        final Map<K, V> result = new HashMap<K, V>();
        final Collection<Integer> partitions = getPartitionsForKeys(ks);
        try {
            OperationFactory factory = operationProvider.createGetAllOperationFactory(ks, expiryPolicy);
            OperationService operationService = getNodeEngine().getOperationService();
            Map<Integer, Object> responses = operationService.invokeOnPartitions(getServiceName(), factory, partitions);
            for (Object response : responses.values()) {
                final Object responseObject = serializationService.toObject(response);
                final Set<Map.Entry<Data, Data>> entries = ((MapEntrySet) responseObject).getEntrySet();
                for (Map.Entry<Data, Data> entry : entries) {
                    final V value = serializationService.toObject(entry.getValue());
                    final K key = serializationService.toObject(entry.getKey());
                    result.put(key, value);
                }
            }
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
        return result;
    }

    @Override
    public void put(K key, V value, ExpiryPolicy expiryPolicy) {
        final InternalCompletableFuture<Object> f = putAsyncInternal(key, value, expiryPolicy, false, true);
        try {
            f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public V getAndPut(K key, V value, ExpiryPolicy expiryPolicy) {
        final InternalCompletableFuture<V> f = putAsyncInternal(key, value, expiryPolicy, true, true);
        try {
            return f.get();
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
            return f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        final Future<Boolean> f = replaceAsyncInternal(key, oldValue, newValue, expiryPolicy, true, false, true);
        try {
            return f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean replace(K key, V value, ExpiryPolicy expiryPolicy) {
        final Future<Boolean> f = replaceAsyncInternal(key, null, value, expiryPolicy, false, false, true);
        try {
            return f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy) {
        final Future<V> f = replaceAsyncInternal(key, null, value, expiryPolicy, false, true, true);
        try {
            return f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public int size() {
        ensureOpen();
        try {
            final SerializationService serializationService = getNodeEngine().getSerializationService();
            OperationFactory operationFactory = operationProvider.createSizeOperationFactory();
            final Map<Integer, Object> results = getNodeEngine().getOperationService()
                    .invokeOnAllPartitions(getServiceName(), operationFactory);
            int total = 0;
            for (Object result : results.values()) {
                total += (Integer) serializationService.toObject(result);
            }
            return total;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    @Override
    public CacheStatistics getLocalCacheStatistics() {
        final ICacheService service = getService();
        return service.createCacheStatIfAbsent(cacheConfig.getNameWithPrefix());
    }

    //endregion

    private Set<Integer> getPartitionsForKeys(Set<Data> keys) {
        final InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        final int partitions = partitionService.getPartitionCount();
        final int capacity = Math.min(partitions, keys.size());
        final Set<Integer> partitionIds = new HashSet<Integer>(capacity);

        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext() && partitionIds.size() < partitions) {
            final Data key = iterator.next();
            partitionIds.add(partitionService.getPartitionId(key));
        }
        return partitionIds;
    }

}
