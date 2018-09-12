/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.CacheEntryProcessorResult;
import com.hazelcast.cache.impl.CacheEventListenerAdaptor;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.CacheEntryProcessorCodec;
import com.hazelcast.client.impl.protocol.codec.CacheEventJournalReadCodec;
import com.hazelcast.client.impl.protocol.codec.CacheEventJournalSubscribeCodec;
import com.hazelcast.client.impl.protocol.codec.CacheEventJournalSubscribeCodec.ResponseParameters;
import com.hazelcast.client.impl.protocol.codec.CacheListenerRegistrationCodec;
import com.hazelcast.client.impl.protocol.codec.CacheLoadAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemovePartitionLostListenerCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.impl.client.PortableReadResultSet;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.function.Predicate;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.cache.CacheEventType.PARTITION_LOST;
import static com.hazelcast.cache.impl.CacheProxyUtil.NULL_KEY_IS_NOT_ALLOWED;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateConfiguredTypes;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.util.ExceptionUtil.rethrowAllowedTypeFirst;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.Preconditions.checkNotNull;

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
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class ClientCacheProxy<K, V> extends AbstractClientCacheProxy<K, V>
        implements EventJournalReader<EventJournalCacheEvent<K, V>> {

    private ClientMessageDecoder eventJournalReadResponseDecoder;
    private ClientMessageDecoder eventJournalSubscribeResponseDecoder;


    ClientCacheProxy(CacheConfig<K, V> cacheConfig, ClientContext context) {
        super(cacheConfig, context);
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();
        eventJournalReadResponseDecoder = new ClientMessageDecoder() {
            @Override
            public ReadResultSet<?> decodeClientMessage(ClientMessage message) {
                final CacheEventJournalReadCodec.ResponseParameters params = CacheEventJournalReadCodec.decodeResponse(message);
                final PortableReadResultSet<?> resultSet = new PortableReadResultSet<Object>(
                        params.readCount, params.items, params.itemSeqs,
                        params.nextSeqExist ? params.nextSeq : ReadResultSet.SEQUENCE_UNAVAILABLE);
                resultSet.setSerializationService(getSerializationService());
                return resultSet;
            }
        };
        eventJournalSubscribeResponseDecoder = new ClientMessageDecoder() {
            @Override
            public EventJournalInitialSubscriberState decodeClientMessage(ClientMessage message) {
                final ResponseParameters resp = CacheEventJournalSubscribeCodec.decodeResponse(message);
                return new EventJournalInitialSubscriberState(resp.oldestSequence, resp.newestSequence);
            }
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

    protected boolean containsKeyInternal(Object key) {
        Data keyData = toData(key);
        ClientMessage request = CacheContainsKeyCodec.encodeRequest(nameWithPrefix, keyData);
        ClientMessage result = invoke(request, keyData);
        return CacheContainsKeyCodec.decodeResponse(result).response;
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        ensureOpen();
        validateNotNull(keys);

        List<Data> dataKeys = new ArrayList<Data>(keys.size());
        for (K key : keys) {
            validateNotNull(key);
            validateConfiguredTypes(cacheConfig, key);

            dataKeys.add(toData(key));
        }

        loadAllInternal(keys, dataKeys, replaceExistingValues, completionListener);
    }

    protected void loadAllInternal(Set<? extends K> keys, List<Data> dataKeys, boolean replaceExistingValues,
                                   CompletionListener completionListener) {
        ClientMessage request = CacheLoadAllCodec.encodeRequest(nameWithPrefix, dataKeys, replaceExistingValues);
        try {
            submitLoadAllTask(request, completionListener, dataKeys);
        } catch (Exception e) {
            if (completionListener != null) {
                completionListener.onException(e);
            }
            throw new CacheException(e);
        }
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

        List<Data> dataKeys = new ArrayList<Data>(keys.size());
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
            return clazz.cast(cacheConfig.getAsReadOnly());
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

    protected Object invokeInternal(Object key, Data epData, Object... arguments) {
        Data keyData = toData(key);
        List<Data> argumentsData;
        if (arguments != null) {
            argumentsData = new ArrayList<Data>(arguments.length);
            for (int i = 0; i < arguments.length; i++) {
                argumentsData.add(toData(arguments[i]));
            }
        } else {
            argumentsData = Collections.emptyList();
        }
        int completionId = nextCompletionId();
        ClientMessage request = CacheEntryProcessorCodec.encodeRequest(nameWithPrefix, keyData, epData, argumentsData,
                completionId);
        try {
            ICompletableFuture<ClientMessage> future = invoke(request, keyData, completionId);
            ClientMessage response = getSafely(future);
            Data data = CacheEntryProcessorCodec.decodeResponse(response).response;
            // at client side, we don't know what entry processor does so we ignore it from statistics perspective
            return toObject(data);
        } catch (CacheException ce) {
            throw ce;
        } catch (Exception e) {
            throw new EntryProcessorException(e);
        }
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
    public CacheManager getCacheManager() {
        return cacheManagerRef.get();
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
        CacheEventListenerAdaptor<K, V> adaptor = new CacheEventListenerAdaptor<K, V>(this, cacheEntryListenerConfiguration,
                getSerializationService());
        EventHandler handler = createHandler(adaptor);
        String regId = getContext().getListenerService().registerListener(createCacheEntryListenerCodec(), handler);
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

    private ListenerMessageCodec createCacheEntryListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return CacheAddEntryListenerCodec.encodeRequest(nameWithPrefix, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return CacheAddEntryListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return CacheRemoveEntryListenerCodec.encodeRequest(nameWithPrefix, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return CacheRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
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

    protected void updateCacheListenerConfigOnOtherNodes(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration,
                                                         boolean isRegister) {
        Collection<Member> members = getContext().getClusterService().getMemberList();
        for (Member member : members) {
            try {
                Address address = member.getAddress();
                Data configData = toData(cacheEntryListenerConfiguration);
                ClientMessage request = CacheListenerRegistrationCodec.encodeRequest(nameWithPrefix, configData, isRegister,
                        address);
                ClientInvocation invocation = new ClientInvocation(getClient(), request, getName(), address);
                invocation.invoke();
            } catch (Exception e) {
                sneakyThrow(e);
            }
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        ensureOpen();
        return new ClientClusterWideIterator<K, V>(this, getContext(), false);
    }

    @Override
    public Iterator<Entry<K, V>> iterator(int fetchSize) {
        ensureOpen();
        return new ClientClusterWideIterator<K, V>(this, getContext(), fetchSize, false);
    }

    @Override
    public Iterator<Entry<K, V>> iterator(int fetchSize, int partitionId, boolean prefetchValues) {
        ensureOpen();
        return new ClientCachePartitionIterator<K, V>(this, getContext(), fetchSize, partitionId, prefetchValues);
    }

    @Override
    public String addPartitionLostListener(CachePartitionLostListener listener) {
        EventHandler<ClientMessage> handler = new ClientCachePartitionLostEventHandler(listener);
        injectDependencies(listener);
        return getContext().getListenerService().registerListener(createPartitionLostListenerCodec(), handler);
    }

    private ListenerMessageCodec createPartitionLostListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return CacheAddPartitionLostListenerCodec.encodeRequest(name, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return CacheAddPartitionLostListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return CacheRemovePartitionLostListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return CacheRemovePartitionLostListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    public boolean removePartitionLostListener(String id) {
        return getContext().getListenerService().deregisterListener(id);
    }


    @Override
    public ICompletableFuture<EventJournalInitialSubscriberState> subscribeToEventJournal(int partitionId) {
        final ClientMessage request = CacheEventJournalSubscribeCodec.encodeRequest(nameWithPrefix);
        final ClientInvocationFuture fut = new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
        return new ClientDelegatingFuture<EventJournalInitialSubscriberState>(fut, getSerializationService(),
                eventJournalSubscribeResponseDecoder);
    }

    @Override
    public <T> ICompletableFuture<ReadResultSet<T>> readFromEventJournal(
            long startSequence,
            int minSize,
            int maxSize,
            int partitionId,
            Predicate<? super EventJournalCacheEvent<K, V>> predicate,
            Projection<? super EventJournalCacheEvent<K, V>, ? extends T> projection
    ) {
        if (maxSize < minSize) {
            throw new IllegalArgumentException("maxSize " + maxSize
                    + " must be greater or equal to minSize " + minSize);
        }
        final SerializationService ss = getSerializationService();
        final ClientMessage request = CacheEventJournalReadCodec.encodeRequest(
                nameWithPrefix, startSequence, minSize, maxSize, ss.toData(predicate), ss.toData(projection));
        final ClientInvocationFuture fut = new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
        return new ClientDelegatingFuture<ReadResultSet<T>>(fut, ss, eventJournalReadResponseDecoder);
    }

    private final class ClientCachePartitionLostEventHandler
            extends CacheAddPartitionLostListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private CachePartitionLostListener listener;

        private ClientCachePartitionLostEventHandler(CachePartitionLostListener listener) {
            this.listener = listener;
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }

        @Override
        public void handleCachePartitionLostEventV10(int partitionId, String uuid) {
            Member member = getContext().getClusterService().getMember(uuid);
            listener.partitionLost(new CachePartitionLostEvent(name, member, PARTITION_LOST.getType(), partitionId));
        }
    }
}
