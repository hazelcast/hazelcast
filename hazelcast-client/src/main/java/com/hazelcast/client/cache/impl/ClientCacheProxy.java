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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.CacheEntryProcessorResult;
import com.hazelcast.cache.impl.CacheEventListenerAdaptor;
import com.hazelcast.cache.impl.CacheEventType;
import com.hazelcast.cache.impl.CacheProxyUtil;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddPartitionLostListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.CacheEntryProcessorCodec;
import com.hazelcast.client.impl.protocol.codec.CacheListenerRegistrationCodec;
import com.hazelcast.client.impl.protocol.codec.CacheLoadAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemovePartitionLostListenerCodec;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;

/**
 * ICache implementation for client
 * <p/>
 * This proxy is the implementation of ICache and javax.cache.Cache which is returned by
 * HazelcastClientCacheManager. Represent a cache on client.
 * <p/>
 * This implementation is a thin proxy implementation using hazelcast client infrastructure
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ClientCacheProxy<K, V>
        extends AbstractClientCacheProxy<K, V> {

    public ClientCacheProxy(CacheConfig<K, V> cacheConfig) {
        super(cacheConfig);
    }

    public NearCache getNearCache() {
        return nearCache;
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
        final Data keyData = toData(key);
        Object cached = nearCache != null ? nearCache.get(keyData) : null;
        if (cached != null && !NearCache.NULL_OBJECT.equals(cached)) {
            return true;
        }
        ClientMessage request = CacheContainsKeyCodec.encodeRequest(nameWithPrefix, keyData);
        ClientMessage result = invoke(request, keyData);
        return CacheContainsKeyCodec.decodeResponse(result).response;
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        ensureOpen();
        validateNotNull(keys);
        for (K key : keys) {
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key);
        }
        HashSet<Data> keysData = new HashSet<Data>();
        for (K key : keys) {
            keysData.add(toData(key));
        }
        ClientMessage request = CacheLoadAllCodec.encodeRequest(nameWithPrefix, keysData, replaceExistingValues);
        try {
            submitLoadAllTask(request, completionListener, keysData);
        } catch (Exception e) {
            if (completionListener != null) {
                completionListener.onException(e);
            }
            throw new CacheException(e);
        }
    }

    @Override
    protected void onLoadAll(Set<Data> keys, Object response, long start, long end) {
        if (statisticsEnabled) {
            // We don't know how many of keys are actually loaded so we assume that all of them are loaded
            // and calculates statistics based on this assumption.
            statistics.increaseCachePuts(keys.size());
            statistics.addPutTimeNanos(end - start);
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
        final long start = System.nanoTime();
        final ICompletableFuture<Boolean> f = removeAsyncInternal(key, null, false, true, false);
        try {
            boolean removed = f.get();
            if (statisticsEnabled) {
                handleStatisticsOnRemove(false, start, removed);
            }
            return removed;
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean remove(K key, V oldValue) {
        final long start = System.nanoTime();
        final ICompletableFuture<Boolean> f = removeAsyncInternal(key, oldValue, true, true, false);
        try {
            boolean removed = f.get();
            if (statisticsEnabled) {
                handleStatisticsOnRemove(false, start, removed);
            }
            return removed;
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public V getAndRemove(K key) {
        final long start = System.nanoTime();
        final ICompletableFuture<V> f = getAndRemoveAsyncInternal(key, true, false);
        try {
            V removedValue = toObject(f.get());
            if (statisticsEnabled) {
                handleStatisticsOnRemove(true, start, removedValue);
            }
            return removedValue;
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
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
        removeAllKeysInternal(keys);
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
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
            throws EntryProcessorException {
        ensureOpen();
        validateNotNull(key);
        if (entryProcessor == null) {
            throw new NullPointerException("Entry Processor is null");
        }
        final Data keyData = toData(key);
        Data epData = toData(entryProcessor);
        List<Data> argumentsData = null;
        if (arguments != null) {
            argumentsData = new ArrayList<Data>(arguments.length);
            for (int i = 0; i < arguments.length; i++) {
                argumentsData.add(toData(arguments[i]));
            }
        }
        final int completionId = nextCompletionId();
        ClientMessage request =
                CacheEntryProcessorCodec.encodeRequest(nameWithPrefix, keyData, epData, argumentsData, completionId);
        try {
            final ICompletableFuture<ClientMessage> f = invoke(request, keyData, completionId);
            final ClientMessage response = getSafely(f);
            final Data data = CacheEntryProcessorCodec.decodeResponse(response).response;
            // At client side, we don't know what entry processor does so we ignore it from statistics perspective
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
        // TODO Implement a multiple (batch) invoke operation and its factory
        ensureOpen();
        validateNotNull(keys);
        if (entryProcessor == null) {
            throw new NullPointerException("Entry Processor is null");
        }
        Map<K, EntryProcessorResult<T>> allResult = new HashMap<K, EntryProcessorResult<T>>();
        for (K key : keys) {
            CacheEntryProcessorResult<T> ceResult;
            try {
                final T result = invoke(key, entryProcessor, arguments);
                ceResult = result != null ? new CacheEntryProcessorResult<T>(result) : null;
            } catch (Exception e) {
                ceResult = new CacheEntryProcessorResult<T>(e);
            }
            if (ceResult != null) {
                allResult.put(key, ceResult);
            }
        }
        // At client side, we don't know what entry processor does so we ignore it from statistics perspective
        return allResult;
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
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
    public void registerCacheEntryListener(CacheEntryListenerConfiguration cacheEntryListenerConfiguration, boolean addToConfig) {
        ensureOpen();
        if (cacheEntryListenerConfiguration == null) {
            throw new NullPointerException("CacheEntryListenerConfiguration can't be null");
        }
        CacheEventListenerAdaptor<K, V> adaptor =
                new CacheEventListenerAdaptor<K, V>(this,
                        cacheEntryListenerConfiguration,
                        clientContext.getSerializationService(),
                        clientContext.getHazelcastInstance());
        EventHandler handler = createHandler(adaptor);
        String regId = clientContext.getListenerService().registerListener(createCacheEntryListenerCodec(), handler);
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
        final String regId = getListenerIdLocal(cacheEntryListenerConfiguration);
        if (regId == null) {
            return;
        }

        ClientListenerService listenerService = clientContext.getListenerService();
        boolean isDeregistered = listenerService.deregisterListener(regId);

        if (isDeregistered) {
            removeListenerLocally(cacheEntryListenerConfiguration);
            cacheConfig.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
            updateCacheListenerConfigOnOtherNodes(cacheEntryListenerConfiguration, false);
        }
    }

    protected void updateCacheListenerConfigOnOtherNodes(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration,
                                                         boolean isRegister) {
        final Collection<Member> members = clientContext.getClusterService().getMemberList();
        final HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
        for (Member member : members) {
            try {
                final Address address = member.getAddress();
                Data configData = toData(cacheEntryListenerConfiguration);
                final ClientMessage request =
                        CacheListenerRegistrationCodec.encodeRequest(nameWithPrefix, configData, isRegister, address);
                final ClientInvocation invocation = new ClientInvocation(client, request, address);
                invocation.invoke();
            } catch (Exception e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        ensureOpen();
        return new ClientClusterWideIterator<K, V>(this, clientContext, false);
    }

    @Override
    public Iterator<Entry<K, V>> iterator(int fetchSize) {
        ensureOpen();
        return new ClientClusterWideIterator<K, V>(this, clientContext, fetchSize, false);
    }

    public Iterator<Entry<K, V>> iterator(int fetchSize, int partitionId, boolean prefetchValues) {
        ensureOpen();
        return new ClientCachePartitionIterator<K, V>(this, clientContext, fetchSize, partitionId, prefetchValues);
    }

    @Override
    public String addPartitionLostListener(CachePartitionLostListener listener) {
        EventHandler<ClientMessage> handler = new ClientCachePartitionLostEventHandler(listener);
        injectDependencies(listener);
        return clientContext.getListenerService().registerListener(createPartitionLostListenerCodec(), handler);
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
        return clientContext.getListenerService().deregisterListener(id);
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
        public void handle(int partitionId, String uuid) {
            final Member member = clientContext.getClusterService().getMember(uuid);
            listener.partitionLost(new CachePartitionLostEvent(name, member, CacheEventType.PARTITION_LOST.getType(),
                    partitionId));
        }

    }

}
