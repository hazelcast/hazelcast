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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostEventFilter;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.cache.impl.event.InternalCachePartitionLostListenerAdapter;
import com.hazelcast.cache.impl.journal.CacheEventJournalReadOperation;
import com.hazelcast.cache.impl.journal.CacheEventJournalSubscribeOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.internal.config.CacheConfigReadOnly;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.ExceptionUtil.rethrowAllowedTypeFirst;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.MapUtil.toIntSize;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static java.util.Collections.emptyMap;

/**
 * <h1>ICache implementation</h1>
 * <p>
 * This proxy is the implementation of ICache and javax.cache.Cache which is returned by
 * HazelcastServerCacheManager. It represents a cache for server or embedded mode.
 * </p>
 * <p>
 * Each cache method actually is an operation which is sent to related partition(s) or node(s).
 * Operations are executed on partition's or node's executor pools and the results are delivered to the user.
 * </p>
 * <p>
 * In order to access a {@linkplain CacheProxy} by name, a cacheManager should be used. It's advised to use
 * {@link com.hazelcast.cache.ICache} instead.
 * </p>
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class CacheProxy<K, V> extends CacheProxySupport<K, V>
        implements EventJournalReader<EventJournalCacheEvent<K, V>> {

    private static final CacheStatistics EMPTY_CACHE_STATS = new CacheStatisticsImpl(Clock.currentTimeMillis());

    CacheProxy(CacheConfig<K, V> cacheConfig, NodeEngine nodeEngine, ICacheService cacheService) {
        super(cacheConfig, nodeEngine, cacheService);
    }

    @Override
    public V get(K key) {
        return get(key, null);
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        return getAll(keys, null);
    }

    @Override
    public boolean containsKey(K key) {
        ensureOpen();
        validateNotNull(key);
        Data dataKey = serializationService.toData(key);
        Operation operation = operationProvider.createContainsKeyOperation(dataKey);
        OperationService operationService = getNodeEngine().getOperationService();
        int partitionId = getPartitionId(dataKey);
        InvocationFuture<Boolean> future = operationService.invokeOnPartition(getServiceName(), operation, partitionId);
        return future.joinInternal();
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        ensureOpen();
        validateNotNull(keys);
        for (K key : keys) {
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key);
        }
        Set<Data> keysData = createHashSet(keys.size());
        for (K key : keys) {
            validateNotNull(key);
            keysData.add(serializationService.toData(key));
        }
        createAndSubmitLoadAllTask(keysData, replaceExistingValues, completionListener);
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
        try {
            InternalCompletableFuture<Boolean> future = removeAsyncInternal(key, null, false, false, true);
            return future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean remove(K key, V oldValue) {
        try {
            InternalCompletableFuture<Boolean> future = removeAsyncInternal(key, oldValue, true, false, true);
            return future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public V getAndRemove(K key) {
        try {
            InternalCompletableFuture<V> future = removeAsyncInternal(key, null, false, true, true);
            return future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return replace(key, oldValue, newValue, null);
    }

    @Override
    public boolean replace(K key, V value) {
        return replace(key, value, (ExpiryPolicy) null);
    }

    @Override
    public V getAndReplace(K key, V value) {
        return getAndReplace(key, value, null);
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        ensureOpen();
        validateNotNull(keys);
        if (keys.isEmpty()) {
            return;
        }
        removeAllInternal(keys);
    }

    @Override
    public void removeAll() {
        ensureOpen();
        removeAllInternal(null);
    }

    @Override
    public void clear() {
        ensureOpen();
        clearInternal();
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(cacheConfig)) {
            return clazz.cast(new CacheConfigReadOnly<>(cacheConfig));
        }
        throw new IllegalArgumentException("The configuration class " + clazz + " is not supported by this implementation");
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        ensureOpen();
        validateNotNull(key);
        checkNotNull(entryProcessor, "Entry Processor is null");
        Data keyData = serializationService.toData(key);
        return invokeInternal(keyData, entryProcessor, arguments);
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
                                                         Object... arguments) {
        // TODO: implement a multiple (batch) invoke operation and its factory
        ensureOpen();
        validateNotNull(keys);
        checkNotNull(entryProcessor, "Entry Processor is null");
        Map<K, EntryProcessorResult<T>> allResult = createHashMap(keys.size());
        for (K key : keys) {
            validateNotNull(key);
            CacheEntryProcessorResult<T> ceResult;
            try {
                T result = invoke(key, entryProcessor, arguments);
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
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(((Object) this).getClass())) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("Unwrapping to " + clazz + " is not supported by this implementation");
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        registerCacheEntryListener(cacheEntryListenerConfiguration, true);
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration,
                                           boolean addToConfig) {
        ensureOpen();
        checkNotNull(cacheEntryListenerConfiguration, "CacheEntryListenerConfiguration can't be null");

        CacheEventListenerAdaptor<K, V> entryListener = new CacheEventListenerAdaptor<K, V>(this,
                cacheEntryListenerConfiguration,
                getNodeEngine().getSerializationService());
        UUID regId = getService().registerListener(getDistributedObjectName(), entryListener, entryListener);
        if (regId != null) {
            if (addToConfig) {
                cacheConfig.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
            }
            addListenerLocally(regId, cacheEntryListenerConfiguration);
            if (addToConfig) {
                updateCacheListenerConfigOnOtherNodes(cacheEntryListenerConfiguration, true);
            }
        }
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        checkNotNull(cacheEntryListenerConfiguration, "CacheEntryListenerConfiguration can't be null");

        UUID regId = getListenerIdLocal(cacheEntryListenerConfiguration);
        if (regId != null) {
            if (getService().deregisterListener(getDistributedObjectName(), regId)) {
                removeListenerLocally(cacheEntryListenerConfiguration);
                cacheConfig.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
                updateCacheListenerConfigOnOtherNodes(cacheEntryListenerConfiguration, false);
            }
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        ensureOpen();
        return new CacheIterator<>(this, false);
    }

    @Override
    public Iterator<Entry<K, V>> iterator(int fetchSize) {
        ensureOpen();
        return new CacheIterator<>(this, fetchSize, false);
    }

    @Override
    public Iterator<Entry<K, V>> iterator(int fetchSize, int partitionId, boolean prefetchValues) {
        ensureOpen();
        return new CachePartitionIterator<>(this, fetchSize, partitionId, prefetchValues);
    }

    @Override
    public Iterable<Entry<K, V>> iterable(int fetchSize, int partitionId, boolean prefetchValues) {
        ensureOpen();
        return new CachePartitionIterable<>(this, fetchSize, partitionId, prefetchValues);
    }

    @Override
    public UUID addPartitionLostListener(CachePartitionLostListener listener) {
        checkNotNull(listener, "CachePartitionLostListener can't be null");

        EventFilter filter = new CachePartitionLostEventFilter();
        listener = injectDependencies(listener);
        InternalCachePartitionLostListenerAdapter listenerAdapter = new InternalCachePartitionLostListenerAdapter(listener);
        EventRegistration registration = getService().getNodeEngine().getEventService()
                .registerListener(AbstractCacheService.SERVICE_NAME, name, filter, listenerAdapter);

        return registration.getId();
    }

    @Override
    public boolean removePartitionLostListener(UUID id) {
        checkNotNull(id, "Listener ID should not be null!");
        return getService().getNodeEngine().getEventService()
                .deregisterListener(AbstractCacheService.SERVICE_NAME, name, id);
    }

    @Override
    public CompletionStage<EventJournalInitialSubscriberState> subscribeToEventJournal(int partitionId) {
        final CacheEventJournalSubscribeOperation op = new CacheEventJournalSubscribeOperation(nameWithPrefix);
        op.setPartitionId(partitionId);
        return getNodeEngine().getOperationService().invokeOnPartition(op);
    }

    @Override
    public <T> CompletionStage<ReadResultSet<T>> readFromEventJournal(
            long startSequence,
            int minSize,
            int maxSize,
            int partitionId,
            Predicate<? super EventJournalCacheEvent<K, V>> predicate,
            Function<? super EventJournalCacheEvent<K, V>, ? extends T> projection
    ) {
        if (maxSize < minSize) {
            throw new IllegalArgumentException("maxSize " + maxSize
                    + " must be greater or equal to minSize " + minSize);
        }
        final CacheEventJournalReadOperation<K, V, T> op = new CacheEventJournalReadOperation<>(
                nameWithPrefix, startSequence, minSize, maxSize, predicate, projection);
        op.setPartitionId(partitionId);
        return getNodeEngine().getOperationService().invokeOnPartition(op);
    }

    @Override
    public CacheStatistics getLocalCacheStatistics() {
        if (!cacheConfig.isStatisticsEnabled()) {
            return EMPTY_CACHE_STATS;
        }
        return getService().createCacheStatIfAbsent(cacheConfig.getNameWithPrefix());
    }

    @Override
    public InternalCompletableFuture<V> getAsync(K key) {
        return getAsync(key, null);
    }

    @Override
    public InternalCompletableFuture<V> getAsync(K key, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        validateNotNull(key);
        Data keyData = serializationService.toData(key);
        Operation op = operationProvider.createGetOperation(keyData, expiryPolicy);
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
    public InternalCompletableFuture<V> getAndPutAsync(K key, V value) {
        return getAndPutAsync(key, value, null);
    }

    @Override
    public InternalCompletableFuture<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy) {
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
    public InternalCompletableFuture<V> getAndRemoveAsync(K key) {
        return removeAsyncInternal(key, null, false, true, false);
    }

    @Override
    public InternalCompletableFuture<Boolean> replaceAsync(K key, V value) {
        return replaceAsyncInternal(key, null, value, null, false, false, false);
    }

    @Override
    public InternalCompletableFuture<Boolean> replaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, null, value, expiryPolicy, false, false, false);
    }

    @Override
    public InternalCompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
        return replaceAsyncInternal(key, oldValue, newValue, null, true, false, false);
    }

    @Override
    public InternalCompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, oldValue, newValue, expiryPolicy, true, false, false);
    }

    @Override
    public InternalCompletableFuture<V> getAndReplaceAsync(K key, V value) {
        return replaceAsyncInternal(key, null, value, null, false, true, false);
    }

    @Override
    public InternalCompletableFuture<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, null, value, expiryPolicy, false, true, false);
    }

    @Override
    public V get(K key, ExpiryPolicy expiryPolicy) {
        try {
            Future<V> future = getAsync(key, expiryPolicy);
            return future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        validateNotNull(keys);
        if (keys.isEmpty()) {
            return emptyMap();
        }
        final int keyCount = keys.size();
        final Set<Data> ks = createHashSet(keyCount);
        for (K key : keys) {
            validateNotNull(key);
            Data dataKey = serializationService.toData(key);
            ks.add(dataKey);
        }
        Map<K, V> result = createHashMap(keyCount);
        PartitionIdSet partitions = getPartitionsForKeys(ks);
        try {
            OperationFactory factory = operationProvider.createGetAllOperationFactory(ks, expiryPolicy);
            OperationService operationService = getNodeEngine().getOperationService();
            Map<Integer, Object> responses = operationService.invokeOnPartitions(getServiceName(), factory, partitions);
            for (Object response : responses.values()) {
                MapEntries mapEntries = serializationService.toObject(response);
                mapEntries.putAllToMap(serializationService, result);
            }
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
        return result;
    }

    @Override
    public void put(K key, V value, ExpiryPolicy expiryPolicy) {
        try {
            InternalCompletableFuture<Object> future = putAsyncInternal(key, value, expiryPolicy, false, true);
            future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public V getAndPut(K key, V value, ExpiryPolicy expiryPolicy) {
        try {
            InternalCompletableFuture<V> future = putAsyncInternal(key, value, expiryPolicy, true, true);
            return future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        validateNotNull(map);

        try {
            // first we fill entry set per partition
            int partitionCount = partitionService.getPartitionCount();
            List<Map.Entry<Data, Data>>[] entriesPerPartition = groupDataToPartitions(map, partitionCount);

            // then we invoke the operations and sync on completion of these operations
            putToAllPartitionsAndWaitForCompletion(entriesPerPartition, expiryPolicy);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public boolean setExpiryPolicy(K key, ExpiryPolicy expiryPolicy) {
        try {
            ensureOpen();
            validateNotNull(key);
            validateNotNull(expiryPolicy);

            Data keyData = serializationService.toData(key);
            Data expiryPolicyData = serializationService.toData(expiryPolicy);
            List<Data> list = Collections.singletonList(keyData);
            Operation operation = operationProvider.createSetExpiryPolicyOperation(list, expiryPolicyData);
            Future<Boolean> future = invoke(operation, keyData, true);
            return future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public void setExpiryPolicy(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        validateNotNull(keys);
        validateNotNull(expiryPolicy);

        try {
            int partitionCount = partitionService.getPartitionCount();
            List<Data>[] keysPerPartition = groupDataToPartitions(keys, partitionCount);
            setTTLAllPartitionsAndWaitForCompletion(keysPerPartition, serializationService.toData(expiryPolicy));
        } catch (Exception e) {
            rethrow(e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map.Entry<Data, Data>>[] groupDataToPartitions(Map<? extends K, ? extends V> map, int partitionCount) {
        List<Map.Entry<Data, Data>>[] entriesPerPartition = new List[partitionCount];

        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            K key = entry.getKey();
            V value = entry.getValue();
            validateNotNull(key, value);

            Data keyData = serializationService.toData(key);
            Data valueData = serializationService.toData(value);

            int partitionId = partitionService.getPartitionId(keyData);
            List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
            if (entries == null) {
                entries = new ArrayList<>();
                entriesPerPartition[partitionId] = entries;
            }

            entries.add(new AbstractMap.SimpleImmutableEntry<>(keyData, valueData));
        }

        return entriesPerPartition;
    }

    @Override
    public boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy) {
        try {
            Future<Boolean> future = putIfAbsentAsyncInternal(key, value, expiryPolicy, true);
            return future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        try {
            Future<Boolean> future = replaceAsyncInternal(key, oldValue, newValue, expiryPolicy, true, false, true);
            return future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean replace(K key, V value, ExpiryPolicy expiryPolicy) {
        try {
            Future<Boolean> future = replaceAsyncInternal(key, null, value, expiryPolicy, false, false, true);
            return future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy) {
        try {
            Future<V> future = replaceAsyncInternal(key, null, value, expiryPolicy, false, true, true);
            return future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public int size() {
        ensureOpen();
        try {
            OperationFactory operationFactory = operationProvider.createSizeOperationFactory();
            Map<Integer, Object> results = getNodeEngine().getOperationService()
                                                          .invokeOnAllPartitions(getServiceName(), operationFactory);
            long total = 0;
            for (Object result : results.values()) {
                total += (Integer) getNodeEngine().toObject(result);
            }
            return toIntSize(total);
        } catch (Throwable t) {
            throw rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }
}
