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
import com.hazelcast.cache.impl.operation.CacheLoadAllOperationFactory;
import com.hazelcast.cache.impl.operation.CachePutIfAbsentOperation;
import com.hazelcast.cache.impl.operation.CachePutOperation;
import com.hazelcast.cache.impl.operation.CacheRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheReplaceOperation;
import com.hazelcast.cache.impl.operation.CacheSizeOperationFactory;
import com.hazelcast.cache.impl.operation.CacheStatisticsOperationFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InitializingObject;
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
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CompletionListener;
import javax.cache.management.CacheMXBean;
import javax.cache.management.CacheStatisticsMXBean;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


/**
 * @author mdogan 05/02/14
 */
final class CacheProxy<K, V> extends AbstractDistributedObject<CacheService> implements ICache<K, V>, InitializingObject {

    private static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    private static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    private final CacheConfig<K, V> cacheConfig;

    private String name;
    private boolean isClosed = false;

    private HazelcastCacheManager cacheManager = null;
    private CacheLoader<K, V> cacheLoader;

    protected CacheProxy(String name, NodeEngine nodeEngine, CacheService service) {
        super(nodeEngine, service);
        this.name = name;
        cacheConfig = nodeEngine.getConfig().findCacheConfig(name);
    }

    //region InitializingObject
    @Override
    public void initialize() {
        //initializeListeners
        for (CacheEntryListenerConfiguration<K, V> listenerConfiguration : cacheConfig.getCacheEntryListenerConfigurations()) {
            registerCacheEntryListener(listenerConfiguration);
        }

        if (this.cacheConfig.getCacheLoaderFactory() != null) {
            cacheLoader = this.cacheConfig.getCacheLoaderFactory().create();
        }
    }
    //endregion

    //region DISTRIBUTED OBJECT
    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }
    //endregion

    //region ICACHE: JCACHE EXTENSION
    @Override
    public Future<V> getAsync(K key, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        final NodeEngine engine = getNodeEngine();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final SerializationService serializationService = engine.getSerializationService();

        final Data k = serializationService.toData(key);
        final Operation op = new CacheGetOperation(name, k, expiryPolicy);
        final InternalCompletableFuture<Object> f = engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));
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
        return putAsyncInternal(key, value, null, false);
    }

    @Override
    public void put(K key, V value, ExpiryPolicy expiryPolicy) {
        putAsyncInternal(key, value, expiryPolicy, false).getSafely();
    }

    @Override
    public Future<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return putAsyncInternal(key, value, expiryPolicy, false);
    }

    @Override
    public InternalCompletableFuture<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        final NodeEngine engine = getNodeEngine();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        final SerializationService serializationService = engine.getSerializationService();

        final Data k = serializationService.toData(key);
        final Data v = serializationService.toData(value);

        final Operation op = new CachePutIfAbsentOperation(name, k, v, expiryPolicy);
        return engine.getOperationService().invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));
    }

    @Override
    public Future<V> getAndPutAsync(K key, V value) {
        return getAndPutAsync(key, value, null);
    }

    @Override
    public V getAndPut(K key, V value, ExpiryPolicy expiryPolicy) {
        final Future<V> f = getAndPutAsync(key, value);
        try {
            return f.get();
        } catch (Throwable e) {
            return ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public Future<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        final InternalCompletableFuture<Object> f = putAsyncInternal(key, value, expiryPolicy, true);
        final NodeEngine nodeEngine = getNodeEngine();
        final SerializationService serializationService = nodeEngine.getSerializationService();
        return new DelegatingFuture<V>(f, serializationService);
    }

    @Override
    public InternalCompletableFuture<Boolean> removeAsync(K key) {
        return removeAsync(key, null, false);
    }

    @Override
    public InternalCompletableFuture<Boolean> removeAsync(K key, V oldValue) {
        return removeAsync(key, oldValue, true);
    }

    private InternalCompletableFuture<Boolean> removeAsync(K key, V oldValue, boolean hasOldValue) {
        ensureOpen();
        final NodeEngine engine = getNodeEngine();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (hasOldValue && oldValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(hasOldValue, key, oldValue);

        final SerializationService serializationService = engine.getSerializationService();
        final Data keyData = serializationService.toData(key);
        final Data valueData = oldValue != null ? serializationService.toData(oldValue) : null;
        final Operation op = new CacheRemoveOperation(name, keyData, valueData);
        final int partitionId = getPartitionId(engine, keyData);
        return engine.getOperationService().invokeOnPartition(getServiceName(), op, partitionId);
    }

    @Override
    public Future<V> getAndRemoveAsync(K key) {
        ensureOpen();
        final NodeEngine engine = getNodeEngine();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final SerializationService serializationService = engine.getSerializationService();
        final Data k = serializationService.toData(key);
        final Operation op = new CacheGetAndRemoveOperation(name, k);
        final InternalCompletableFuture<Object> f =
                engine.getOperationService().invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));
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
        final NodeEngine engine = getNodeEngine();
        final SerializationService serializationService = engine.getSerializationService();

        final Data k = serializationService.toData(key);
        final Data v = serializationService.toData(value);

        final Operation op = new CacheGetAndReplaceOperation(name, k, v, expiryPolicy);
        final InternalCompletableFuture<Object> f = engine.getOperationService().invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));
        return new DelegatingFuture<V>(f, serializationService);
    }

    @Override
    public InternalCompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return replaceAsync(key, oldValue, newValue, expiryPolicy, true);
    }

    private InternalCompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy, boolean hasOldValue) {
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

        final NodeEngine engine = getNodeEngine();
        final SerializationService serializationService = engine.getSerializationService();

        final Data k = serializationService.toData(key);
        final Data o = serializationService.toData(oldValue);
        final Data n = serializationService.toData(newValue);

        final Operation op = new CacheReplaceOperation(name, k, o, n, expiryPolicy);
        return engine.getOperationService().invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));
    }

    @Override
    public V get(K key, ExpiryPolicy expiryPolicy) {
        final Future<V> f = getAsync(key, expiryPolicy);
        try {
            return f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        final NodeEngine engine = getNodeEngine();
        final SerializationService serializationService = engine.getSerializationService();

        if (keys == null || keys.contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }

        final Set<Data> ks = new HashSet(keys.size());
        for (K key : keys) {
            final Data k = serializationService.toData(key);
            ks.add(k);
        }
        if (keys.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
        final Map<K, V> result = new HashMap<K, V>();

        final Collection<Integer> partitions = getPartitionsForKeys(ks);
        try {
            final CacheGetAllOperationFactory factory = new CacheGetAllOperationFactory(name, ks, expiryPolicy);
            final Map<Integer, Object> responses = engine.getOperationService()
                    .invokeOnPartitions(getServiceName(), factory, partitions);
            for (Object response : responses.values()) {
                final Object responseObject = serializationService.toObject(response);
                final Set<Map.Entry<Data, Data>> entries = ((MapEntrySet) responseObject).getEntrySet();
                for (Map.Entry<Data, Data> entry : entries) {
                    final V value = serializationService.toObject(entry.getValue());
                    final K key = serializationService.toObject(entry.getKey());
                    result.put(key, value);
//                    if (nearCacheEnabled) {
//                        int partitionId = nodeEngine.getPartitionService().getPartitionId(entry.getKey());
//                        if (!nodeEngine.getPartitionService().getPartitionOwner(partitionId)
//                                .equals(nodeEngine.getClusterService().getThisAddress()) || mapConfig.getNearCacheConfig().isCacheLocalEntries()) {
//                            mapService.putNearCache(name, entry.getKey(), entry.getValue());
//                        }
//                    }
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
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
        final InternalCompletableFuture<Boolean> f = putIfAbsentAsync(key, value, expiryPolicy);
        return f.getSafely();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        final InternalCompletableFuture<Boolean> f = replaceAsync(key, oldValue, newValue, expiryPolicy);
        return f.getSafely();
    }

    @Override
    public boolean replace(K key, V value, ExpiryPolicy expiryPolicy) {
        final InternalCompletableFuture<Boolean> f = replaceAsync(key, null, value, expiryPolicy, false);
        return f.getSafely();
    }

    @Override
    public V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy) {
        final Future<V> f = getAndReplaceAsync(key, value, expiryPolicy);
        try {
            return f.get();
        } catch (Throwable e) {
            return ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public int size() {
        ensureOpen();
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            final SerializationService serializationService = nodeEngine.getSerializationService();
            final CacheSizeOperationFactory operationFactory = new CacheSizeOperationFactory(name);
            final Map<Integer, Object> results =
                    nodeEngine.getOperationService().invokeOnAllPartitions(getServiceName(), operationFactory);
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
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public CacheMXBean getCacheMXBean() {
        return null;
    }

    @Override
    public CacheStatisticsMXBean getCacheStatisticsMXBean() {
        ensureOpen();
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            final SerializationService serializationService = nodeEngine.getSerializationService();
            final CacheStatisticsOperationFactory operationFactory = new CacheStatisticsOperationFactory(name);
            final Map<Integer, Object> results =
                    nodeEngine.getOperationService().invokeOnAllPartitions(getServiceName(), operationFactory);

            CacheStatistics total = new CacheStatistics();
            for (Object result : results.values()) {
                CacheStatistics stat;
                if (result instanceof Data) {
                    stat = serializationService.toObject((Data) result);
                } else {
                    stat = (CacheStatistics) result;
                }
                if(stat != null){
                    total = total.acumulate(stat);
                }
            }
            return new CacheStatisticsMXBeanImpl(total);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public void setStatisticsEnabled(boolean enabled) {
//        throw new UnsupportedOperationException();
        if (enabled) {
            MXBeanUtil.registerCacheObject(this, true);
        } else {
            MXBeanUtil.unregisterCacheObject(this, true);
        }
    }

    public V get(Data key) {
        ensureOpen();
        final NodeEngine engine = getNodeEngine();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final SerializationService serializationService = engine.getSerializationService();

        final Operation op = new CacheGetOperation(name, key, null);
        final InternalCompletableFuture<Object> f = engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, getPartitionId(engine, key));
        Object result = f.getSafely();
        if (result instanceof Data) {
            result = serializationService.toObject((Data) result);
        }
        return (V) result;
    }

    public void enableManagement(boolean enabled){
//        if(enabled){
//            getNodeEngine().
//        }
    }

    void initCacheManager(HazelcastCacheManager cacheManager) {
        if (this.cacheManager == null) {
            this.cacheManager = cacheManager;
        }
    }

    protected void postDestroy() {
        this.isClosed = true;
    }


    private <T> InternalCompletableFuture<T> putAsyncInternal(K key, V value,
                                                              ExpiryPolicy expiryPolicy, boolean getValue) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);

        final NodeEngine engine = getNodeEngine();
        final SerializationService serializationService = engine.getSerializationService();

        final Data keyData = serializationService.toData(key);
        final Data valueData = serializationService.toData(value);

        final Operation op = new CachePutOperation(name, keyData, valueData, expiryPolicy, getValue);
        final int partitionId = getPartitionId(engine, keyData);
        return engine.getOperationService().invokeOnPartition(getServiceName(), op, partitionId);
    }

    private static int getPartitionId(NodeEngine nodeEngine, Data key) {
        return nodeEngine.getPartitionService().getPartitionId(key);
    }

    private void validateConfiguredTypes(boolean validateValues, K key, V... values) throws ClassCastException {
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
    //endregion

    //region javax.cache.Cache<K, V> IMPL

    @Override
    public V get(K key) {
        final Future<V> f = getAsync(key);
        try {
            return f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public boolean remove(Data key) {
        ensureOpen();
        final NodeEngine engine = getNodeEngine();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final Operation op = new CacheRemoveOperation(name, key, null);
        final int partitionId = getPartitionId(engine, key);
        final InternalCompletableFuture<Boolean> f = engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, partitionId);
        return f.getSafely();
    }

    private Collection<Integer> getPartitionsForKeys(Set<Data> keys) {
        final InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        final int partitions = partitionService.getPartitionCount();
        final int capacity = Math.min(partitions, keys.size()); //todo: is there better way to estimate size?
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
        final NodeEngine engine = getNodeEngine();
        final SerializationService serializationService = engine.getSerializationService();

        final Data k = serializationService.toData(key);
        final Operation op = new CacheContainsKeyOperation(name, k);
        final InternalCompletableFuture<Boolean> f = engine.getOperationService()
                .invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));

        return f.getSafely();
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        ensureOpen();

        final NodeEngine nodeEngine = getNodeEngine();
        final OperationService operationService = nodeEngine.getOperationService();

        if (keys == null || keys.contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }

        for(K key:keys){
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

        OperationFactory operationFactory = new CacheLoadAllOperationFactory(name, keysData,replaceExistingValues);
        try {
            final Map<Integer, Object> results = operationService.invokeOnAllPartitions(getServiceName(), operationFactory);

            for (Object result : results.values()) {
                if (result != null && result instanceof CacheClearResponse) {
                    final Object response = ((CacheClearResponse) result).getResponse();
                    if (response instanceof Exception) {
                        if (completionListener != null) {
                            completionListener.onException((Exception) response);
                            return;
                        }
                    }
                }
            }

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
        final InternalCompletableFuture<Boolean> f = putIfAbsentAsync(key, value, null);
        return f.getSafely();
    }

    @Override
    public boolean remove(K key) {
        final InternalCompletableFuture<Boolean> f = removeAsync(key);
        return f.getSafely();
    }

    @Override
    public boolean remove(K key, V oldValue) {
        final InternalCompletableFuture<Boolean> f = removeAsync(key, oldValue);
        return f.getSafely();
    }

    @Override
    public V getAndRemove(K key) {
        final Future<V> f = getAndRemoveAsync(key);
        try {
            return f.get();
        } catch (Throwable e) {
            return ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        final InternalCompletableFuture<Boolean> f = replaceAsync(key, oldValue, newValue, null, true);
        return f.getSafely();
    }

    @Override
    public boolean replace(K key, V value) {
        final InternalCompletableFuture<Boolean> f = replaceAsync(key, null, value, null, false);
        return f.getSafely();
    }

    @Override
    public V getAndReplace(K key, V value) {
        final Future<V> f = getAndReplaceAsync(key, value, null);
        try {
            return f.get();
        } catch (Throwable e) {
            return ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        ensureOpen();
        final NodeEngine nodeEngine = getNodeEngine();
        if (keys == null || keys.contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final SerializationService ss = nodeEngine.getSerializationService();
        HashSet<Data> keysData = new HashSet<Data>();
        for (K key : keys) {
            keysData.add(ss.toData(key));
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
        final OperationService operationService = getNodeEngine().getOperationService();
        final CacheClearOperationFactory operationFactory = new CacheClearOperationFactory(name, keysData, isRemoveAll);
        try {
            final Map<Integer, Object> results = operationService.invokeOnAllPartitions(getServiceName(), operationFactory);
            for (Object result : results.values()) {
                if (result != null && result instanceof CacheClearResponse) {
                    final Object response = ((CacheClearResponse) result).getResponse();
                    if (response instanceof Throwable) {
                        throw (Throwable) response;
                    }
                }
            }
        } catch (Throwable t) {
            throw rethrow(t, CacheException.class);
        }

    }

    //TODO MOVE THIS INTO ExceptionUtil
    private static <T extends Throwable> RuntimeException rethrow(final Throwable t, Class<T> allowedType) throws T {
        if (t instanceof Error) {
            if (t instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) t);
            }
            throw (Error) t;
        } else if (allowedType.isAssignableFrom(t.getClass())) {
            throw (T) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof ExecutionException) {
            final Throwable cause = t.getCause();
            if (cause != null) {
                throw rethrow(cause, allowedType);
            } else {
                throw new HazelcastException(t);
            }
        } else {
            throw new HazelcastException(t);
        }
    }


    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(cacheConfig)) {
            return clazz.cast(cacheConfig);
        }
        throw new IllegalArgumentException("The configuration class " + clazz +
                " is not supported by this implementation");
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        ensureOpen();
        final NodeEngine engine = getNodeEngine();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (entryProcessor == null) {
            throw new NullPointerException();
        }
        final SerializationService serializationService = engine.getSerializationService();
        final Data k = serializationService.toData(key);
        final Operation op = new CacheEntryProcessorOperation(name, k, entryProcessor, arguments);
        try {
            final InternalCompletableFuture<T> f = engine.getOperationService()
                    .invokeOnPartition(getServiceName(), op, getPartitionId(engine, k));
            return f.getSafely();
        } catch (CacheException ce) {
            throw ce;
        } catch (Exception e) {
            throw new EntryProcessorException(e);
        }
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
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
                ceResult = new CacheEntryProcessorResult<T>(result);
            } catch (Exception e) {
                ceResult = new CacheEntryProcessorResult<T>(e);
            }
            allResult.put(key, ceResult);
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

        destroy();
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
        final CacheService service = getService();
        service.registerCacheEntryListener(this, cacheEntryListenerConfiguration);
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        final CacheService service = getService();
        service.deregisterCacheEntryListener(this, cacheEntryListenerConfiguration);
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        ensureOpen();
        return new ClusterWideIterator<K, V>(this);
    }

    //endregion


}
