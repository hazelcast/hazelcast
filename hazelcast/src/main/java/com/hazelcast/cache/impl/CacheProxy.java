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

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.operation.CacheClearOperationFactory;
import com.hazelcast.cache.impl.operation.CacheContainsKeyOperation;
import com.hazelcast.cache.impl.operation.CacheEntryProcessorOperation;
import com.hazelcast.cache.impl.operation.CacheGetAllOperationFactory;
import com.hazelcast.cache.impl.operation.CacheGetAndRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheGetAndReplaceOperation;
import com.hazelcast.cache.impl.operation.CacheGetOperation;
import com.hazelcast.cache.impl.operation.CacheListenerRegistrationOperation;
import com.hazelcast.cache.impl.operation.CacheLoadAllOperationFactory;
import com.hazelcast.cache.impl.operation.CachePutIfAbsentOperation;
import com.hazelcast.cache.impl.operation.CachePutOperation;
import com.hazelcast.cache.impl.operation.CacheRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheReplaceOperation;
import com.hazelcast.cache.impl.operation.CacheSizeOperationFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.DelegatingFuture;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CacheProxy implementing ICache
 *
 * @param <K> key
 * @param <V> value
 */
public class CacheProxy<K, V>
        implements ICache<K, V> {
    //WARNING:: this proxy do not extend AbstractDistributedObject as Cache and AbstractDistributedObject
    // has getName method which have different values a distributedObject delegate used to over come this
    private static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    private static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";

    private static final int IGNORE_COMPLETION = -1;

    private final CacheConfig<K, V> cacheConfig;
    //this will represent the name from the user perspective
    private final String name;
    private boolean isClosed;

    private CacheDistributedObject delegate;

    private HazelcastCacheManager cacheManager;
    private CacheLoader<K, V> cacheLoader;

    private ConcurrentHashMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations;
    private ConcurrentHashMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations;
    private ConcurrentHashMap<Integer, CountDownLatch> syncLocks;

    private AtomicInteger completionIdCounter = new AtomicInteger();
    private String completionRegistrationId;

    private final NodeEngine nodeEngine;
    private final SerializationService serializationService;

    protected CacheProxy(CacheConfig cacheConfig, CacheDistributedObject delegate, HazelcastServerCacheManager cacheManager) {
        this.name = cacheConfig.getName();
        this.cacheConfig = cacheConfig;
        this.delegate = delegate;
        this.cacheManager = cacheManager;
        if (cacheConfig.getCacheLoaderFactory() != null) {
            final Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
            cacheLoader = cacheLoaderFactory.create();
        }
        asyncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncLocks = new ConcurrentHashMap<Integer, CountDownLatch>();

        this.nodeEngine = delegate.getNodeEngine();
        serializationService = this.nodeEngine.getSerializationService();
    }

    private static int getPartitionId(NodeEngine nodeEngine, Data key) {
        return nodeEngine.getPartitionService().getPartitionId(key);
    }

    public static void loadAllHelper(Map<Integer, Object> results) {
        for (Object result : results.values()) {
            if (result != null && result instanceof CacheClearResponse) {
                final Object response = ((CacheClearResponse) result).getResponse();
                if (response instanceof Throwable) {
                    //                    if (completionListener != null) {
                    //                        completionListener.onException((Exception) response);
                    //                    }
                    ExceptionUtil.sneakyThrow((Throwable) response);
                }
            }
        }
    }

    public String getDistributedObjectName() {
        return delegate.getName();
    }

    //region DISTRIBUTED OBJECT
    protected String getServiceName() {
        return delegate.getServiceName();
    }
    //endregion

    protected CacheService getService() {
        return delegate.getService();
    }

    protected NodeEngine getNodeEngine() {
        return delegate.getNodeEngine();
    }

    //region ICACHE: JCACHE EXTENSION
    @Override
    public Future<V> getAsync(K key, ExpiryPolicy expiryPolicy) {
        ensureOpen();
//        final NodeEngine engine = getNodeEngine();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
//        final SerializationService serializationService = engine.getSerializationService();

        final Data k = serializationService.toData(key);
        final Operation op = new CacheGetOperation(getDistributedObjectName(), k, expiryPolicy);
        final InternalCompletableFuture<Object> f = nodeEngine.getOperationService()
                                                          .invokeOnPartition(getServiceName(), op, getPartitionId(nodeEngine, k));
        return new DelegatingFuture<V>(f, serializationService);
    }

    void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    @Override
    public Future<V> getAsync(K key) {
        return getAsync(key, null);
    }

    @Override
    public Future<Void> putAsync(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        return putAsyncInternal(key, value, null, false, IGNORE_COMPLETION);
    }

    @Override
    public void put(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);

        final Integer completionId = registerCompletionLatch(1);

        final InternalCompletableFuture<Object> f =  putAsyncInternal(key, value, expiryPolicy, false, completionId);
        try {
            f.get();
            waitCompletionLatch(completionId);
        } catch (Throwable e) {
            deregisterCompletionLatch(completionId);
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public Future<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        return putAsyncInternal(key, value, expiryPolicy, false, IGNORE_COMPLETION);
    }

    @Override
    public InternalCompletableFuture<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
//        final NodeEngine engine = getNodeEngine();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);

        final Data k = serializationService.toData(key);
        final Data v = serializationService.toData(value);

        final Operation op = new CachePutIfAbsentOperation(getDistributedObjectName(), k, v, expiryPolicy, IGNORE_COMPLETION);
        return nodeEngine.getOperationService().invokeOnPartition(getServiceName(), op, getPartitionId(nodeEngine, k));
    }

    @Override
    public Future<V> getAndPutAsync(K key, V value) {
        return getAndPutAsync(key, value, null);
    }

    @Override
    public V getAndPut(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);

        final Integer completionId = registerCompletionLatch(1);
        final InternalCompletableFuture<Object> f = putAsyncInternal(key, value, expiryPolicy, true,completionId);
        try {
            final Object oldValueData = f.get();
            waitCompletionLatch(completionId);
            return serializationService.toObject(oldValueData);
        } catch (Throwable e) {
            deregisterCompletionLatch(completionId);
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public Future<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        final InternalCompletableFuture<Object> f = putAsyncInternal(key, value, expiryPolicy, true, IGNORE_COMPLETION);
        final SerializationService serializationService = nodeEngine.getSerializationService();
        return new DelegatingFuture<V>(f, serializationService);
    }

    @Override
    public InternalCompletableFuture<Boolean> removeAsync(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(false, key);
        return removeAsyncInternal(key, null, IGNORE_COMPLETION);
    }

    @Override
    public InternalCompletableFuture<Boolean> removeAsync(K key, V oldValue) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (oldValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, oldValue);
        return removeAsyncInternal(key, oldValue, IGNORE_COMPLETION);
    }

    private InternalCompletableFuture<Boolean> removeAsyncInternal(K key, V oldValue, int completionId) {
        final SerializationService serializationService = nodeEngine.getSerializationService();
        final Data keyData = serializationService.toData(key);
        final Data valueData = oldValue != null ? serializationService.toData(oldValue) : null;
        final Operation op = new CacheRemoveOperation(getDistributedObjectName(), keyData, valueData, completionId);
        final int partitionId = getPartitionId(nodeEngine, keyData);
        return nodeEngine.getOperationService().invokeOnPartition(getServiceName(), op, partitionId);
    }

    @Override
    public Future<V> getAndRemoveAsync(K key) {
        ensureOpen();
//        final NodeEngine engine = getNodeEngine();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
//        final SerializationService serializationService = nodeEngine.getSerializationService();
        final Data k = serializationService.toData(key);
        final Operation op = new CacheGetAndRemoveOperation(getDistributedObjectName(), k, IGNORE_COMPLETION);
        final InternalCompletableFuture<Object> f = nodeEngine.getOperationService()
                                                          .invokeOnPartition(getServiceName(), op, getPartitionId(nodeEngine, k));
        return new DelegatingFuture<V>(f, serializationService);
    }

    @Override
    public Future<Boolean> replaceAsync(K key, V oldValue, V newValue) {
        return replaceAsync(key, oldValue, newValue, null);
    }

    @Override
    public DelegatingFuture<V> getAndReplaceAsync(K key, V value) {
        return getAndReplaceAsync(key, value, null);
    }

    @Override
    public DelegatingFuture<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);

        final Data k = serializationService.toData(key);
        final Data v = serializationService.toData(value);

        final Operation op = new CacheGetAndReplaceOperation(getDistributedObjectName(), k, v, expiryPolicy, IGNORE_COMPLETION);
        final InternalCompletableFuture<Object> f = nodeEngine.getOperationService()
                                                          .invokeOnPartition(getServiceName(), op, getPartitionId(nodeEngine, k));
        return new DelegatingFuture<V>(f, serializationService);
    }

    @Override
    public InternalCompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, oldValue, newValue, expiryPolicy, true);
    }

    private InternalCompletableFuture<Boolean> replaceAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                                    boolean hasOldValue) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (newValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        if (hasOldValue && oldValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        if (hasOldValue) {
            validateConfiguredTypes(true, key, oldValue, newValue);
        } else {
            validateConfiguredTypes(true, key, newValue);
        }

        final Data k = serializationService.toData(key);
        final Data o = serializationService.toData(oldValue);
        final Data n = serializationService.toData(newValue);

        final Operation op = new CacheReplaceOperation(getDistributedObjectName(), k, o, n, expiryPolicy, IGNORE_COMPLETION);
        return nodeEngine.getOperationService().invokeOnPartition(getServiceName(), op, getPartitionId(nodeEngine, k));
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

        if (keys == null || keys.contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (keys.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
//        final NodeEngine engine = getNodeEngine();
//        final SerializationService serializationService = engine.getSerializationService();

        final Set<Data> ks = new HashSet(keys.size());
        for (K key : keys) {
            final Data k = serializationService.toData(key);
            ks.add(k);
        }
        final Map<K, V> result = new HashMap<K, V>();

        final Collection<Integer> partitions = getPartitionsForKeys(ks);
        try {
            final CacheGetAllOperationFactory factory = new CacheGetAllOperationFactory(getDistributedObjectName(), ks,
                    expiryPolicy);
            final Map<Integer, Object> responses = nodeEngine.getOperationService()
                                                         .invokeOnPartitions(getServiceName(), factory, partitions);
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
    public void putAll(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (map == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (map.keySet().contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (map.values().contains(null)) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }

        final Iterator<? extends Map.Entry<? extends K, ? extends V>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<? extends K, ? extends V> next = iter.next();
            put(next.getKey(), next.getValue(), expiryPolicy);
        }
    }

    @Override
    public boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);

        final Integer completionId = registerCompletionLatch(1);
        final Data k = serializationService.toData(key);
        final Data v = serializationService.toData(value);
        final Operation op = new CachePutIfAbsentOperation(getDistributedObjectName(), k, v, expiryPolicy, completionId);
        final InternalCompletableFuture<Boolean> f = nodeEngine.getOperationService().invokeOnPartition(getServiceName(), op, getPartitionId(nodeEngine, k));
        final Boolean isPut = f.getSafely();
        waitCompletionLatch(completionId);
        return isPut;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (newValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        if (oldValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, oldValue, newValue);

        final Data k = serializationService.toData(key);
        final Data o = serializationService.toData(oldValue);
        final Data n = serializationService.toData(newValue);

        final Integer completionId = registerCompletionLatch(1);
        final Operation op = new CacheReplaceOperation(getDistributedObjectName(), k, o, n, expiryPolicy, completionId);
        final InternalCompletableFuture<Boolean> f = nodeEngine.getOperationService().invokeOnPartition(getServiceName(), op, getPartitionId(nodeEngine, k));
        final Boolean isReplaced = f.getSafely();
        waitCompletionLatch(completionId);
        return isReplaced;
    }

    @Override
    public boolean replace(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);

        final Data k = serializationService.toData(key);
        final Data n = serializationService.toData(value);

        final Integer completionId = registerCompletionLatch(1);
        final Operation op = new CacheReplaceOperation(getDistributedObjectName(), k, null, n, expiryPolicy, completionId);
        final InternalCompletableFuture<Boolean> f = nodeEngine.getOperationService().invokeOnPartition(getServiceName(), op, getPartitionId(nodeEngine, k));
        final Boolean isReplaced = f.getSafely();
        waitCompletionLatch(completionId);
        return isReplaced;
    }

    @Override
    public V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);

        final Data k = serializationService.toData(key);
        final Data v = serializationService.toData(value);

        final Integer completionId = registerCompletionLatch(1);

        final Operation op = new CacheGetAndReplaceOperation(getDistributedObjectName(), k, v, expiryPolicy, completionId);
        final InternalCompletableFuture<Object> f = nodeEngine.getOperationService()
                                                              .invokeOnPartition(getServiceName(), op,
                                                                      getPartitionId(nodeEngine, k));
        try {
            final Object oldValue = f.get();
            waitCompletionLatch(completionId);
            return serializationService.toObject(oldValue);
        } catch (Throwable e) {
            deregisterCompletionLatch(completionId);
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public int size() {
        ensureOpen();
//        final NodeEngine nodeEngine = getNodeEngine();
        try {
            final SerializationService serializationService = nodeEngine.getSerializationService();
            final CacheSizeOperationFactory operationFactory = new CacheSizeOperationFactory(getDistributedObjectName());
            final Map<Integer, Object> results = nodeEngine.getOperationService()
                                                           .invokeOnAllPartitions(getServiceName(), operationFactory);
            int total = 0;
            for (Object result : results.values()) {
                Integer size;
                if (result instanceof Data) {
                    size = serializationService.toObject((Data) result);
                } else {
                    size = (Integer) result;
                }
                total += size;
            }
            return total;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    public V get(Data key) {
        ensureOpen();
//        final NodeEngine engine = getNodeEngine();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final SerializationService serializationService = nodeEngine.getSerializationService();

        final Operation op = new CacheGetOperation(getDistributedObjectName(), key, null);
        final InternalCompletableFuture<Object> f = nodeEngine.getOperationService()
                                                          .invokeOnPartition(getServiceName(), op, getPartitionId(nodeEngine, key));
        Object result = f.getSafely();
        if (result instanceof Data) {
            result = serializationService.toObject((Data) result);
        }
        return (V) result;
    }

    private <T> InternalCompletableFuture<T> putAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy, boolean getValue , int completionId) {
        final Data keyData = serializationService.toData(key);
        final Data valueData = serializationService.toData(value);

        final Operation op = new CachePutOperation(getDistributedObjectName(), keyData, valueData, expiryPolicy, getValue, completionId);
        final int partitionId = getPartitionId(nodeEngine, keyData);
        return nodeEngine.getOperationService().invokeOnPartition(getServiceName(), op, partitionId);
    }
    //endregion

    //region javax.cache.Cache<K, V> IMPL

    private void validateConfiguredTypes(boolean validateValues, K key, V... values)
            throws ClassCastException {
        final Class keyType = cacheConfig.getKeyType();
        final Class valueType = cacheConfig.getValueType();
        if (Object.class != keyType) {
            //means type checks required
            if (!keyType.isAssignableFrom(key.getClass())) {
                throw new ClassCastException("Key " + key + "is not assignable to " + keyType);
            }
        }
        if (validateValues) {
            for (V value : values) {
                if (Object.class != valueType) {
                    //means type checks required
                    if (!valueType.isAssignableFrom(value.getClass())) {
                        throw new ClassCastException("Value " + value + "is not assignable to " + valueType);
                    }
                }
            }
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public V get(K key) {
        final Future<V> f = getAsync(key);
        try {
            return f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

//    public boolean remove(Data key) {
//        ensureOpen();
//        if (key == null) {
//            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
//        }
//        final Operation op = new CacheRemoveOperation(getDistributedObjectName(), key, null, IGNORE_COMPLETION);
//        final int partitionId = getPartitionId(nodeEngine, key);
//        final InternalCompletableFuture<Boolean> f = nodeEngine.getOperationService()
//                                                           .invokeOnPartition(getServiceName(), op, partitionId);
//        return f.getSafely();
//    }

    private Collection<Integer> getPartitionsForKeys(Set<Data> keys) {
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final int partitions = partitionService.getPartitionCount();
        //todo: is there better way to estimate size?
        final int capacity = Math.min(partitions, keys.size());
        final Set<Integer> partitionIds = new HashSet<Integer>(capacity);

        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext() && partitionIds.size() < partitions) {
            final Data key = iterator.next();
            partitionIds.add(partitionService.getPartitionId(key));
        }
        return partitionIds;
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        return getAll(keys, null);
    }

    @Override
    public boolean containsKey(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
//        final NodeEngine engine = getNodeEngine();
//        final SerializationService serializationService = engine.getSerializationService();

        final Data k = serializationService.toData(key);
        final Operation op = new CacheContainsKeyOperation(getDistributedObjectName(), k);
        final InternalCompletableFuture<Boolean> f = nodeEngine.getOperationService()
                                                           .invokeOnPartition(getServiceName(), op, getPartitionId(nodeEngine, k));

        return f.getSafely();
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        ensureOpen();
//        final NodeEngine nodeEngine = getNodeEngine();
        final OperationService operationService = nodeEngine.getOperationService();
        if (keys == null || keys.contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        for (K key : keys) {
            validateConfiguredTypes(false, key);
        }
        if (cacheLoader == null) {
            if (completionListener != null) {
                completionListener.onCompletion();
            }
            return;
        }
        final SerializationService ss = nodeEngine.getSerializationService();
        HashSet<Data> keysData = new HashSet<Data>();
        for (K key : keys) {
            keysData.add(ss.toData(key));
        }
        OperationFactory operationFactory = new CacheLoadAllOperationFactory(getDistributedObjectName(), keysData,
                replaceExistingValues);
        try {
            final Map<Integer, Object> results = operationService.invokeOnAllPartitions(getServiceName(), operationFactory);
            loadAllHelper(results);
            if (completionListener != null) {
                completionListener.onCompletion();
            }
        } catch (Exception e) {
            if (completionListener != null) {
                completionListener.onException(e);
            }
        }
    }

    @Override
    public void put(K key, V value) {
        put(key, value, null);
    }

    @Override
    public V getAndPut(K key, V value) {
        return getAndPut(key, value, null);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        putAll(map, null);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, null);
    }

    @Override
    public boolean remove(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(false, key);
        final Integer completionId = registerCompletionLatch(1);
        final InternalCompletableFuture<Boolean> f = removeAsyncInternal(key, null, completionId);
        final Boolean isRemoved = f.getSafely();
        waitCompletionLatch(completionId);
        return isRemoved;
    }

    @Override
    public boolean remove(K key, V oldValue) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (oldValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, oldValue);
        final Integer completionId = registerCompletionLatch(1);
        final InternalCompletableFuture<Boolean> f = removeAsyncInternal(key, oldValue, completionId);
        final Boolean isRemoved = f.getSafely();
        waitCompletionLatch(completionId);
        return isRemoved;
    }

    @Override
    public V getAndRemove(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(false, key);
        final Data k = serializationService.toData(key);
        final Integer completionId = registerCompletionLatch(1);

        final Operation op = new CacheGetAndRemoveOperation(getDistributedObjectName(), k, completionId);
        final InternalCompletableFuture<Object> f = nodeEngine.getOperationService().invokeOnPartition(getServiceName(), op,
                getPartitionId(nodeEngine, k));
        try {
            final Object oldValue = f.get();
            waitCompletionLatch(completionId);
            return serializationService.toObject(oldValue);
        } catch (Throwable e) {
            deregisterCompletionLatch(completionId);
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return replace(key, oldValue, newValue, null);
    }

    @Override
    public boolean replace(K key, V value) {
        final ExpiryPolicy ep= null;
        return replace(key, value, ep);
    }

    @Override
    public V getAndReplace(K key, V value) {
        return getAndReplace(key, value, null);
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        ensureOpen();
        if (keys == null || keys.contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        HashSet<Data> keysData = new HashSet<Data>();
        for (K key : keys) {
            keysData.add(serializationService.toData(key));
        }
        removeAllInternal(keysData, true);
    }

    @Override
    public void removeAll() {
        ensureOpen();
        removeAllInternal(null, true);
    }

    @Override
    public void clear() {
        ensureOpen();
        removeAllInternal(null, false);
    }

    private void removeAllInternal(Set<Data> keysData, boolean isRemoveAll) {
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        final Integer completionId = registerCompletionLatch(partitionCount);

        final OperationService operationService = nodeEngine.getOperationService();
        final CacheClearOperationFactory operationFactory = new CacheClearOperationFactory(getDistributedObjectName(), keysData,
                isRemoveAll, completionId);
        try {
            final Map<Integer, Object> results = operationService.invokeOnAllPartitions(getServiceName(), operationFactory);
            int completionCount=0;
            for (Object result : results.values()) {
                if (result != null && result instanceof CacheClearResponse) {
                    final Object response = ((CacheClearResponse) result).getResponse();
                    if(response instanceof Boolean){
                        completionCount++;
                    }
                    if (response instanceof Throwable) {
                        throw (Throwable) response;
                    }
                }
            }
            //fix completion count
            final CountDownLatch countDownLatch = syncLocks.get(completionId);
            if (countDownLatch != null) {
                for(int i=0; i< partitionCount-completionCount; i++){
                    countDownLatch.countDown();
                }
            }
            waitCompletionLatch(completionId);
        } catch (Throwable t) {
            deregisterCompletionLatch(completionId);
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(cacheConfig)) {
            return clazz.cast(cacheConfig);
        }
        throw new IllegalArgumentException("The configuration class " + clazz + " is not supported by this implementation");
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
            throws EntryProcessorException {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (entryProcessor == null) {
            throw new NullPointerException();
        }
        final Data k = serializationService.toData(key);
        final Integer completionId = registerCompletionLatch(1);
        final Operation op = new CacheEntryProcessorOperation(getDistributedObjectName(), k, completionId, entryProcessor, arguments);
        try {
            final InternalCompletableFuture<T> f = nodeEngine.getOperationService()
                                                         .invokeOnPartition(getServiceName(), op, getPartitionId(nodeEngine, k));
            final T safely = f.getSafely();
            waitCompletionLatch(completionId);
            return safely;
        } catch (CacheException ce) {
            deregisterCompletionLatch(completionId);
            throw ce;
        } catch (Exception e) {
            deregisterCompletionLatch(completionId);
            throw new EntryProcessorException(e);
        }
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
                                                         Object... arguments) {
        //TODO implement a Multiple invoke operation and its factory
        ensureOpen();
        if (keys == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (entryProcessor == null) {
            throw new NullPointerException();
        }
        Map<K, EntryProcessorResult<T>> allResult = new HashMap<K, EntryProcessorResult<T>>();
        for (K key : keys) {
            CacheEntryProcessorResult<T> ceResult;
            try {
                final T result = this.invoke(key, entryProcessor, arguments);
                ceResult = result != null ? new CacheEntryProcessorResult<T>(result) : null;
            } catch (Exception e) {
                ceResult = new CacheEntryProcessorResult<T>(e);
            }
            if (ceResult != null) {
                allResult.put(key, ceResult);
            }
        }
        return allResult;
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public void close() {
        //TODO CHECK this is valid
/*
        must close and release all resources being coordinated on behalf of the Cache by the
        CacheManager. This includes calling the close method on configured CacheLoader,
                CacheWriter, registered CacheEntryListeners and ExpiryPolicy instances that
        implement the java.io.Closeable interface,
*/
        isClosed = true;
        delegate.destroy();

        //close the configured CacheLoader
        if (cacheLoader instanceof Closeable) {
            try {
                ((Closeable) cacheLoader).close();
            } catch (IOException e) {
                //log
            }
        }
        deregisterCompletionListener();


    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(((Object) this).getClass())) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("Unwrapping to " + clazz + " is not supported by this implementation");
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        ensureOpen();
        if (cacheEntryListenerConfiguration == null) {
            throw new NullPointerException("CacheEntryListenerConfiguration can't be " + "null");
        }
        final CacheService service = getService();

        final CacheEventListenerAdaptor<K, V> entryListener = new CacheEventListenerAdaptor<K, V>(this,
                cacheEntryListenerConfiguration, nodeEngine.getSerializationService());

        final String regId = service.registerListener(getDistributedObjectName(), entryListener);
        if (regId != null) {
            cacheConfig.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
            if(cacheEntryListenerConfiguration.isSynchronous()){
                syncListenerRegistrations.putIfAbsent(cacheEntryListenerConfiguration, regId);
                registerCompletionListener();
            } else {
                asyncListenerRegistrations.putIfAbsent(cacheEntryListenerConfiguration, regId);
            }

            //CREATE ON OTHERS TOO
            final OperationService operationService = nodeEngine.getOperationService();
            final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
            for (MemberImpl member : members) {
                if (!member.localMember()) {
                    final Operation op = new CacheListenerRegistrationOperation(getDistributedObjectName(),
                            cacheEntryListenerConfiguration, true);
                    final InternalCompletableFuture<Object> f2 = operationService
                            .invokeOnTarget(CacheService.SERVICE_NAME, op, member.getAddress());
                    //make sure all configs are created
                    f2.getSafely();
                }
            }
        }
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        if (cacheEntryListenerConfiguration == null) {
            throw new NullPointerException("CacheEntryListenerConfiguration can't be " + "null");
        }
        final CacheService service = getService();
        final ConcurrentHashMap<CacheEntryListenerConfiguration, String> regs;
        if(cacheEntryListenerConfiguration.isSynchronous()){
            regs=syncListenerRegistrations;
        } else {
            regs=asyncListenerRegistrations;
        }
        final String regId = regs.remove(cacheEntryListenerConfiguration);
        if (regId != null) {
            if (!service.deregisterListener(getDistributedObjectName(), regId)) {
                regs.putIfAbsent(cacheEntryListenerConfiguration, regId);
            } else {
                cacheConfig.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
                deregisterCompletionListener();
                //REMOVE ON OTHERS TOO
                final OperationService operationService = nodeEngine.getOperationService();
                final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
                for (MemberImpl member : members) {
                    if (!member.localMember()) {
                        final Operation op = new CacheListenerRegistrationOperation(getDistributedObjectName(),
                                cacheEntryListenerConfiguration, false);
                        final InternalCompletableFuture<Object> f2 = operationService
                                .invokeOnTarget(CacheService.SERVICE_NAME, op, member.getAddress());
                        //make sure all configs are created
                        f2.getSafely();
                    }
                }
            }
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        ensureOpen();
        return new ClusterWideIterator<K, V>(this);
    }
    //endregion

    //region sync listeners
    private Integer registerCompletionLatch(int count) {
        if (!syncListenerRegistrations.isEmpty()) {
            final int id = completionIdCounter.incrementAndGet();
            CountDownLatch countDownLatch = new CountDownLatch(count);
            syncLocks.put(id, countDownLatch);
            return id;
        }
        return IGNORE_COMPLETION;
    }

    private void deregisterCompletionLatch(Integer countDownLatchId){
        syncLocks.remove(countDownLatchId);
    }

    private void waitCompletionLatch(Integer countDownLatchId) {
        final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
        if (countDownLatch != null) {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void countDownCompletionLatch(int id) {
        final CountDownLatch countDownLatch = syncLocks.get(id);
        countDownLatch.countDown();
        if(countDownLatch.getCount() == 0){
            deregisterCompletionLatch(id);
        }
    }

    private void registerCompletionListener() {
        if(!syncListenerRegistrations.isEmpty() && completionRegistrationId == null){
            final CacheService service = getService();
            CacheEventListener entryListener = new CacheEventListener() {
                @Override
                public void handleEvent(Object eventObject) {
                    if(eventObject instanceof CacheEventData){
                        CacheEventData cacheEventData = (CacheEventData) eventObject;
                        if(cacheEventData.getCacheEventType() == CacheEventType.COMPLETED) {
                            int completionId = serializationService.toObject(cacheEventData.getDataValue());
                            countDownCompletionLatch(completionId);
                        }
                    }

                }
            };
            completionRegistrationId = service.registerListener(getDistributedObjectName(), entryListener);
        }
    }
    private void deregisterCompletionListener() {
        if(syncListenerRegistrations.isEmpty() && completionRegistrationId != null){
            final CacheService service = getService();
            final boolean isDeregistered = service.deregisterListener(getDistributedObjectName(), completionRegistrationId);
            if(isDeregistered){
                completionRegistrationId = null;
            }
        }
    }

    //endregion
}
