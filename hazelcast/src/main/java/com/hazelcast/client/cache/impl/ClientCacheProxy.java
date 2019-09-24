/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.CacheEntryProcessorResult;
import com.hazelcast.cache.impl.CacheEventListenerAdaptor;
import com.hazelcast.cache.impl.CacheSyncListenerCompleter;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheEventJournalReadCodec;
import com.hazelcast.client.impl.protocol.codec.CacheEventJournalSubscribeCodec;
import com.hazelcast.client.impl.protocol.codec.CacheEventJournalSubscribeCodec.ResponseParameters;
import com.hazelcast.client.impl.protocol.codec.CacheSizeCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.config.CacheConfigReadOnly;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.impl.client.PortableReadResultSet;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.hazelcast.cache.impl.CacheProxyUtil.NULL_KEY_IS_NOT_ALLOWED;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateConfiguredTypes;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.client.cache.impl.ClientCacheProxySupportUtil.createCacheEntryListenerCodec;
import static com.hazelcast.client.cache.impl.ClientCacheProxySupportUtil.createHandler;
import static com.hazelcast.client.cache.impl.ClientCacheProxySupportUtil.createPartitionLostListenerCodec;
import static com.hazelcast.internal.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.internal.util.ExceptionUtil.rethrowAllowedTypeFirst;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.emptyMap;

/**
 * {@link com.hazelcast.cache.ICache} implementation for Hazelcast clients.
 * <p>
 * This proxy is the implementation of {@link com.hazelcast.cache.ICache} and {@link javax.cache.Cache} which is returned by
 * {@link HazelcastClientCacheManager}. Represents a cache on client.
 * <p>
 * This implementation is a thin proxy implementation using hazelcast client infrastructure.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ClientCacheProxy<K, V> extends ClientCacheProxySupport<K, V>
        implements EventJournalReader<EventJournalCacheEvent<K, V>>, CacheSyncListenerCompleter {

    private ClientMessageDecoder eventJournalReadResponseDecoder;
    private ClientMessageDecoder eventJournalSubscribeResponseDecoder;


    ClientCacheProxy(CacheConfig<K, V> cacheConfig, ClientContext context) {
        super(cacheConfig, context);
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();
        eventJournalReadResponseDecoder = message -> {
            final CacheEventJournalReadCodec.ResponseParameters params = CacheEventJournalReadCodec.decodeResponse(message);
            final PortableReadResultSet<?> resultSet = new PortableReadResultSet<>(
                    params.readCount, params.items, params.itemSeqs, params.nextSeq);
            resultSet.setSerializationService(getSerializationService());
            return resultSet;
        };
        eventJournalSubscribeResponseDecoder = message -> {
            final ResponseParameters resp = CacheEventJournalSubscribeCodec.decodeResponse(message);
            return new EventJournalInitialSubscriberState(resp.oldestSequence, resp.newestSequence);
        };
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

        return containsKeyInternal(key);
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        ensureOpen();
        validateNotNull(keys);

        List<Data> dataKeys = new ArrayList<>(keys.size());
        for (K key : keys) {
            validateNotNull(key);
            validateConfiguredTypes(cacheConfig, key);

            dataKeys.add(toData(key));
        }

        loadAllInternal(keys, dataKeys, replaceExistingValues, completionListener);
    }

    @Override
    protected void onLoadAll(List<Data> keys, Object response, long startNanos) {
        if (statisticsEnabled) {
            statsHandler.onBatchPut(startNanos, keys.size());
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
        long start = nowInNanosOrDefault();
        try {
            boolean removed = (Boolean) removeAsyncInternal(key, null, false, true, false);
            if (statisticsEnabled) {
                statsHandler.onRemove(false, start, removed);
            }
            return removed;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean remove(K key, V oldValue) {
        long start = nowInNanosOrDefault();
        try {
            boolean removed = (Boolean) removeAsyncInternal(key, oldValue, true, true, false);
            if (statisticsEnabled) {
                statsHandler.onRemove(false, start, removed);
            }
            return removed;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public V getAndRemove(K key) {
        long start = nowInNanosOrDefault();
        ICompletableFuture<V> future = getAndRemoveSyncInternal(key);
        try {
            V removedValue = toObject(future.get());
            if (statisticsEnabled) {
                statsHandler.onRemove(true, start, removedValue);
            }
            return removedValue;
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
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        checkNotNull(keys, NULL_KEY_IS_NOT_ALLOWED);
        if (keys.isEmpty()) {
            return;
        }

        List<Data> dataKeys = new ArrayList<>(keys.size());
        objectToDataCollection(keys, dataKeys, getSerializationService(), NULL_KEY_IS_NOT_ALLOWED);
        removeAllKeysInternal(keys, dataKeys, startNanos);
    }

    @Override
    public void removeAll() {
        ensureOpen();
        removeAllInternal();
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
        if (entryProcessor == null) {
            throw new NullPointerException("Entry Processor is null");
        }

        Data epData = toData(entryProcessor);
        return (T) invokeInternal(key, epData, arguments);
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
                                                         Object... arguments) {
        // TODO: implement a multiple (batch) invoke operation and its factory
        ensureOpen();
        validateNotNull(keys);
        if (entryProcessor == null) {
            throw new NullPointerException("Entry Processor is null");
        }
        Map<K, EntryProcessorResult<T>> allResult = createHashMap(keys.size());
        for (K key : keys) {
            validateNotNull(key);
            CacheEntryProcessorResult<T> cepResult;
            try {
                T result = invoke(key, entryProcessor, arguments);
                cepResult = result != null ? new CacheEntryProcessorResult<T>(result) : null;
            } catch (Exception e) {
                cepResult = new CacheEntryProcessorResult<T>(e);
            }
            if (cepResult != null) {
                allResult.put(key, cepResult);
            }
        }
        // at client side, we don't know what entry processor does so we ignore it from statistics perspective
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
        if (cacheEntryListenerConfiguration == null) {
            throw new NullPointerException("CacheEntryListenerConfiguration can't be null");
        }
        CacheEventListenerAdaptor<K, V> adaptor = new CacheEventListenerAdaptor<>(this, cacheEntryListenerConfiguration,
                getSerializationService());
        EventHandler handler = createHandler(adaptor);
        String regId = getContext().getListenerService().registerListener(createCacheEntryListenerCodec(nameWithPrefix), handler);
        if (regId != null) {
            if (addToConfig) {
                cacheConfig.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
            }
            addListenerLocally(regId, cacheEntryListenerConfiguration, adaptor);
            if (addToConfig) {
                updateCacheListenerConfigOnOtherNodes(cacheEntryListenerConfiguration, true);
            }
        }
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        if (cacheEntryListenerConfiguration == null) {
            throw new NullPointerException("CacheEntryListenerConfiguration can't be null");
        }
        String regId = getListenerIdLocal(cacheEntryListenerConfiguration);
        if (regId == null) {
            return;
        }

        boolean isDeregistered = getContext().getListenerService().deregisterListener(regId);
        if (isDeregistered) {
            removeListenerLocally(cacheEntryListenerConfiguration);
            cacheConfig.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
            updateCacheListenerConfigOnOtherNodes(cacheEntryListenerConfiguration, false);
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        ensureOpen();
        return new ClientClusterWideIterator<>(this, getContext(), false);
    }

    @Override
    public Iterator<Entry<K, V>> iterator(int fetchSize) {
        ensureOpen();
        return new ClientClusterWideIterator<>(this, getContext(), fetchSize, false);
    }

    @Override
    public Iterator<Entry<K, V>> iterator(int fetchSize, int partitionId, boolean prefetchValues) {
        ensureOpen();
        return new ClientCachePartitionIterator<>(this, getContext(), fetchSize, partitionId, prefetchValues);
    }

    @Override
    public String addPartitionLostListener(CachePartitionLostListener listener) {
        EventHandler<ClientMessage> handler = new ClientCacheProxySupportUtil.ClientCachePartitionLostEventHandler(name,
                getContext(), listener);
        injectDependencies(listener);
        return getContext().getListenerService().registerListener(createPartitionLostListenerCodec(name), handler);
    }

    @Override
    public boolean removePartitionLostListener(String id) {
        return getContext().getListenerService().deregisterListener(id);
    }

    @Override
    public ICompletableFuture<EventJournalInitialSubscriberState> subscribeToEventJournal(int partitionId) {
        final ClientMessage request = CacheEventJournalSubscribeCodec.encodeRequest(nameWithPrefix);
        final ClientInvocationFuture fut = new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
        return new ClientDelegatingFuture<>(fut, getSerializationService(),
                eventJournalSubscribeResponseDecoder);
    }

    @Override
    public <T> ICompletableFuture<ReadResultSet<T>> readFromEventJournal(
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
        final SerializationService ss = getSerializationService();
        final ClientMessage request = CacheEventJournalReadCodec.encodeRequest(
                nameWithPrefix, startSequence, minSize, maxSize, ss.toData(predicate), ss.toData(projection));
        final ClientInvocationFuture fut = new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
        return new ClientDelegatingFuture<>(fut, ss, eventJournalReadResponseDecoder);
    }

    @Override
    public ICompletableFuture<V> getAsync(K key) {
        return getAsync(key, null);
    }

    @Override
    public ICompletableFuture<V> getAsync(K key, ExpiryPolicy expiryPolicy) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        validateNotNull(key);

        ExecutionCallback<V> callback = !statisticsEnabled ? null : statsHandler.newOnGetCallback(startNanos);
        return getAsyncInternal(key, expiryPolicy, callback);
    }

    @Override
    public ICompletableFuture<Void> putAsync(K key, V value) {
        return putAsync(key, value, null);
    }

    @Override
    public ICompletableFuture<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return (ICompletableFuture<Void>) putAsyncInternal(key, value, expiryPolicy, false, true,
                newStatsCallbackOrNull(false));
    }

    @Override
    public ICompletableFuture<Boolean> putIfAbsentAsync(K key, V value) {
        return (ICompletableFuture<Boolean>) putIfAbsentInternal(key, value, null, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return (ICompletableFuture<Boolean>) putIfAbsentInternal(key, value, expiryPolicy, false, true);
    }

    @Override
    public ICompletableFuture<V> getAndPutAsync(K key, V value) {
        return getAndPutAsync(key, value, null);
    }

    @Override
    public ICompletableFuture<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return putAsyncInternal(key, value, expiryPolicy, true, false, newStatsCallbackOrNull(true));
    }

    @Override
    public ICompletableFuture<Boolean> removeAsync(K key) {
        return (ICompletableFuture<Boolean>) removeAsyncInternal(key, null, false, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> removeAsync(K key, V oldValue) {
        return (ICompletableFuture<Boolean>) removeAsyncInternal(key, oldValue, true, false, true);
    }

    @Override
    public ICompletableFuture<V> getAndRemoveAsync(K key) {
        return getAndRemoveAsyncInternal(key);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V value) {
        return replaceAsyncInternal(key, null, value, null, false, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, null, value, expiryPolicy, false, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
        return replaceAsyncInternal(key, oldValue, newValue, null, true, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, oldValue, newValue, expiryPolicy, true, false, true);
    }

    @Override
    public ICompletableFuture<V> getAndReplaceAsync(K key, V value) {
        return replaceAndGetAsyncInternal(key, null, value, null, false, false, true);
    }

    @Override
    public ICompletableFuture<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceAndGetAsyncInternal(key, null, value, expiryPolicy, false, false, true);
    }

    @Override
    public V get(K key, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        validateNotNull(key);
        return toObject(getSyncInternal(key, expiryPolicy));
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        checkNotNull(keys, NULL_KEY_IS_NOT_ALLOWED);
        if (keys.isEmpty()) {
            return emptyMap();
        }

        int keysSize = keys.size();
        List<Data> dataKeys = new LinkedList<Data>();
        List<Object> resultingKeyValuePairs = new ArrayList<Object>(keysSize * 2);
        getAllInternal(keys, dataKeys, expiryPolicy, resultingKeyValuePairs, startNanos);

        Map<K, V> result = createHashMap(keysSize);
        for (int i = 0; i < resultingKeyValuePairs.size(); ) {
            K key = toObject(resultingKeyValuePairs.get(i++));
            V value = toObject(resultingKeyValuePairs.get(i++));
            result.put(key, value);
        }
        return result;
    }

    @Override
    public void put(K key, V value, ExpiryPolicy expiryPolicy) {
        putSyncInternal(key, value, expiryPolicy, false);
    }

    @Override
    public V getAndPut(K key, V value, ExpiryPolicy expiryPolicy) {
        return putSyncInternal(key, value, expiryPolicy, true);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void putAll(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        checkNotNull(map, "map is null");
        if (map.isEmpty()) {
            return;
        }
        putAllInternal(map, expiryPolicy, null, new List[partitionCount], startNanos);
    }

    @Override
    public void setExpiryPolicy(Set<? extends K> keys, ExpiryPolicy policy) {
        ensureOpen();
        checkNotNull(keys);
        checkNotNull(policy);
        if (keys.isEmpty()) {
            return;
        }
        setExpiryPolicyInternal(keys, policy);
    }

    @Override
    public boolean setExpiryPolicy(K key, ExpiryPolicy expiryPolicy) {
        return setExpiryPolicyInternal(key, expiryPolicy);
    }


    @Override
    public boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy) {
        return (Boolean) putIfAbsentInternal(key, value, expiryPolicy, true, false);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return replaceSyncInternal(key, oldValue, newValue, expiryPolicy, true);
    }

    @Override
    public boolean replace(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceSyncInternal(key, null, value, expiryPolicy, false);
    }

    @Override
    public V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy) {
        long startNanos = nowInNanosOrDefault();
        Future<V> future = replaceAndGetAsyncInternal(key, null, value, expiryPolicy, false, true, false);
        try {
            V oldValue = future.get();
            if (statisticsEnabled) {
                statsHandler.onReplace(true, startNanos, oldValue);
            }
            return oldValue;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public int size() {
        ensureOpen();
        try {
            ClientMessage request = CacheSizeCodec.encodeRequest(nameWithPrefix);
            ClientMessage resultMessage = invoke(request);
            return CacheSizeCodec.decodeResponse(resultMessage).response;
        } catch (Throwable t) {
            throw rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    @Override
    public CacheStatistics getLocalCacheStatistics() {
        return statsHandler.getStatistics();
    }
}
