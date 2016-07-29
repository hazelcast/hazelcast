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

import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventListenerAdaptor;
import com.hazelcast.cache.impl.CacheProxyUtil;
import com.hazelcast.cache.impl.CacheSyncListenerCompleter;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheExecutor;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheClearCodec;
import com.hazelcast.client.impl.protocol.codec.CacheContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAndRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAndReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutAllCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllKeysCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheReplaceCodec;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.CompletedFuture;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.ExceptionUtil.rethrowAllowedTypeFirst;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.EMPTY_MAP;

/**
 * Abstract {@link com.hazelcast.cache.ICache} implementation which provides shared internal implementations
 * of cache operations like get, put, replace, remove and invoke. {@code ICache} and {@code javax.cache.Cache} method
 * implementations delegate to these internal methods.
 *
 * This class also implements the client-side near cache for ICache/JCache caches.
 * <p/>
 * <p>Note: this partial implementation is used by client.</p>
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
abstract class AbstractClientInternalCacheProxy<K, V>
        extends AbstractClientCacheProxyBase<K, V>
        implements CacheSyncListenerCompleter {

    /*
     * The general pattern for cache operation implementations is:
     * - Implement the functionality in a {name}Async method. Typically this will include validations and
     * dispatching the invocation to a member.
     * - Return an ICompletableFuture<T> from the async method; when near cache or statistics are enabled
     * then this will be a CacheMaintenanceClientDelegatingFuture which supports execution of near cache
     * and statistics maintenance code to be executed after get(), otherwise it will be a plain ClientDelegatingFuture.
     * - When implementing Cache/ICache interface methods, invoke the internal async method and either return
     * the future itself (if an async method) or return its get() result (for a sync method).
     */

    private static final long MAX_COMPLETION_LATCH_WAIT_TIME = TimeUnit.MINUTES.toMillis(5);
    private static final long COMPLETION_LATCH_WAIT_TIME_STEP = TimeUnit.SECONDS.toMillis(1);

    private static final ClientMessageDecoder GET_AND_REMOVE_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheGetAndRemoveCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheGetCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder REMOVE_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) Boolean.valueOf(CacheRemoveCodec.decodeResponse(clientMessage).response);
        }
    };

    private static final ClientMessageDecoder REPLACE_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheReplaceCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_AND_REPLACE_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheGetAndReplaceCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder PUT_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CachePutCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder PUT_IF_ABSENT_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) Boolean.valueOf(CachePutIfAbsentCodec.decodeResponse(clientMessage).response);
        }
    };

    protected HazelcastClientCacheManager cacheManager;
    protected NearCacheManager nearCacheManager;
    // Object => Data or <V>
    protected NearCache<Data, Object> nearCache;
    // essentially nearCacheEnabled = (nearCache != null)
    protected boolean nearCacheEnabled;
    protected String nearCacheMembershipRegistrationId;

    protected ClientCacheStatisticsImpl statistics;
    protected boolean statisticsEnabled;

    protected boolean cacheOnUpdate;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations;
    private final ConcurrentMap<Integer, CountDownLatch> syncLocks;

    protected AbstractClientInternalCacheProxy(CacheConfig cacheConfig) {
        super(cacheConfig);
        this.asyncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        this.syncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        this.syncLocks = new ConcurrentHashMap<Integer, CountDownLatch>();
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        nearCacheManager = clientContext.getNearCacheManager();

        initNearCache();

        if (nearCache != null) {
            statistics = new ClientCacheStatisticsImpl(System.currentTimeMillis(),
                                                       nearCache.getNearCacheStats());
        } else {
            statistics = new ClientCacheStatisticsImpl(System.currentTimeMillis());
        }
        statisticsEnabled = cacheConfig.isStatisticsEnabled();
    }

    void setCacheManager(HazelcastClientCacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    protected void postDestroy() {
        if (cacheManager != null) {
            cacheManager.destroyCache(getName());
        }
    }

    private void initNearCache() {
        NearCacheConfig nearCacheConfig = clientContext.getClientConfig().getNearCacheConfig(name);
        if (nearCacheConfig != null) {
            nearCacheEnabled = true;
            cacheOnUpdate = nearCacheConfig.getLocalUpdatePolicy() == NearCacheConfig.LocalUpdatePolicy.CACHE;
            NearCacheContext nearCacheContext =
                    new NearCacheContext(
                            nearCacheManager,
                            getSerializationService(),
                            createNearCacheExecutor(clientContext.getExecutionService()),
                            null);
            nearCache = nearCacheManager.getOrCreateNearCache(nameWithPrefix, nearCacheConfig, nearCacheContext);
            registerInvalidationListener();
        } else {
            nearCacheEnabled = false;
        }
    }

    // <V> or Data
    protected Object getFromNearCache(Data keyData) {
        Object cached = nearCache != null ? nearCache.get(keyData) : null;
        if (cached != null && NearCache.NULL_OBJECT != cached) {
            return cached;
        }
        return null;
    }

    protected boolean containsKeyInternal(K key) {
        ensureOpen();
        validateNotNull(key);
        final Data keyData = toData(key);
        if (getFromNearCache(keyData) != null) {
            return true;
        } else {
            ClientMessage request = CacheContainsKeyCodec.encodeRequest(nameWithPrefix, keyData);
            ClientMessage result = invoke(request, keyData);
            return CacheContainsKeyCodec.decodeResponse(result).response;
        }
    }

    protected Map<K, V> getAllInternal(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        final long start = System.nanoTime();
        ensureOpen();
        validateNotNull(keys);
        if (keys.isEmpty()) {
            return EMPTY_MAP;
        }
        final Set<Data> keySet = new HashSet<Data>(keys.size());
        for (K key : keys) {
            final Data k = toData(key);
            keySet.add(k);
        }
        // keys found in near cache are removed from keySet by getAllFromNearCache
        Map<K, V> result = getAllFromNearCache(keySet);
        if (keySet.isEmpty()) {
            return result;
        }
        Data expiryPolicyData = toData(expiryPolicy);
        ClientMessage request = CacheGetAllCodec.encodeRequest(nameWithPrefix, keySet, expiryPolicyData);
        ClientMessage responseMessage = invoke(request);
        List<Map.Entry<Data, Data>> entries = CacheGetAllCodec.decodeResponse(responseMessage).response;
        for (Map.Entry<Data, Data> dataEntry : entries) {
            Data keyData = dataEntry.getKey();
            Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            V value = toObject(valueData);
            result.put(key, value);
            storeInNearCache(keyData, valueData, value);
        }
        if (statisticsEnabled) {
            statistics.increaseCacheHits(entries.size());
            statistics.addGetTimeNanos(System.nanoTime() - start);
        }
        return result;
    }

    protected void putAllInternal(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy) {
        final long start = System.nanoTime();

        ensureOpen();
        validateNotNull(map);

        ClientPartitionService partitionService = clientContext.getPartitionService();
        int partitionCount = partitionService.getPartitionCount();

        try {
            // First we fill entry set per partition
            List<Map.Entry<Data, Data>>[] entriesPerPartition =
                    groupDataToPartitions(map, partitionService, partitionCount);

            // Then we invoke the operations and sync on completion of these operations
            putToAllPartitionsAndWaitForCompletion(entriesPerPartition, expiryPolicy, start);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected ICompletableFuture<V> getAsyncInternal(K key, ExpiryPolicy expiryPolicy) {
        final long start = System.nanoTime();
        ensureOpen();
        validateNotNull(key);
        final Data keyData = toData(key);
        Object cached = getFromNearCache(keyData);
        if (cached != null) {
            return createCompletedFuture(cached);
        }
        final Data expiryPolicyData = toData(expiryPolicy);
        ClientMessage request = CacheGetCodec.encodeRequest(nameWithPrefix, keyData, expiryPolicyData);
        ClientInvocationFuture future;
        try {
            final int partitionId = clientContext.getPartitionService().getPartitionId(key);
            final HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
            final ClientInvocation clientInvocation = new ClientInvocation(client, request, partitionId);
            future = clientInvocation.invoke();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        if (isNearCacheOrStatsEnabled()) {
            return new CacheMaintenanceClientDelegatingFuture<V>(future, getSerializationService(), GET_RESPONSE_DECODER,
                    nearCacheEnabled, statisticsEnabled) {

                @Override
                public void handleNearCacheUpdate() {
                    storeInNearCache(keyData, (Data) responseData, deserializedValue);
                }

                @Override
                public void handleStatistics() {
                    handleStatisticsOnGet(start, responseData);
                }
            };
        } else {
            return new ClientDelegatingFuture<V>(future, getSerializationService(), GET_RESPONSE_DECODER);
        }
    }

    protected void handleStatisticsOnGet(long start, Object response) {
        if (response == null) {
            statistics.increaseCacheMisses();
        } else {
            statistics.increaseCacheHits();
        }
        statistics.addGetTimeNanos(System.nanoTime() - start);
    }

    protected Map<K, V> getAllFromNearCache(Set<Data> keySet) {
        Map<K, V> result = new HashMap<K, V>();
        if (nearCacheEnabled) {
            final Iterator<Data> iterator = keySet.iterator();
            while (iterator.hasNext()) {
                Data key = iterator.next();
                Object cached = nearCache.get(key);
                if (cached != null && !NearCache.NULL_OBJECT.equals(cached)) {
                    result.put((K) toObject(key), (V) cached);
                    iterator.remove();
                }
            }
        }
        return result;
    }

    protected List<Map.Entry<Data, Data>>[] groupDataToPartitions(Map<? extends K, ? extends V> map,
                                                                  ClientPartitionService partitionService, int partitionCount) {
        List<Map.Entry<Data, Data>>[] entriesPerPartition = new List[partitionCount];
        SerializationService serializationService = getSerializationService();

        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            K key = entry.getKey();
            V value = entry.getValue();
            validateNotNull(key, value);

            Data keyData = serializationService.toData(key);
            Data valueData = serializationService.toData(value);

            int partitionId = partitionService.getPartitionId(keyData);
            List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
            if (entries == null) {
                entries = new ArrayList<Map.Entry<Data, Data>>();
                entriesPerPartition[partitionId] = entries;
            }

            entries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(keyData, valueData));
        }

        return entriesPerPartition;
    }

    protected void putToAllPartitionsAndWaitForCompletion(List<Map.Entry<Data, Data>>[] entriesPerPartition,
                                                          ExpiryPolicy expiryPolicy, long start)
            throws ExecutionException, InterruptedException {
        Data expiryPolicyData = toData(expiryPolicy);
        List<FutureEntriesTuple> futureEntriesTuples =
                new ArrayList<FutureEntriesTuple>(entriesPerPartition.length);
        for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
            List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
            if (entries != null) {
                int completionId = nextCompletionId();
                // TODO If there is a single entry, we could make use of a put operation since that is a bit cheaper
                ClientMessage request =
                        CachePutAllCodec.encodeRequest(nameWithPrefix, entries, expiryPolicyData, completionId);
                Future f = invoke(request, partitionId, completionId);
                futureEntriesTuples.add(new FutureEntriesTuple(f, entries));
            }
        }

        waitResponseFromAllPartitionsForPutAll(futureEntriesTuples, start);
    }

    private void waitResponseFromAllPartitionsForPutAll(List<FutureEntriesTuple> futureEntriesTuples,
                                                        long start) {
        Throwable error = null;
        for (FutureEntriesTuple tuple : futureEntriesTuples) {
            Future future = tuple.future;
            List<Map.Entry<Data, Data>> entries = tuple.entries;
            try {
                future.get();
                if (nearCacheEnabled) {
                    handleNearCacheOnPutAll(entries);
                }
                // Note that we count the batch put only if there is no exception while putting to target partition.
                // In case of error, some of the entries might have been put and others might fail.
                // But we simply ignore the actual put count here if there is an error.
                if (statisticsEnabled) {
                    statistics.increaseCachePuts(entries.size());
                }
            } catch (Throwable t) {
                if (nearCacheEnabled) {
                    handleNearCacheOnPutAll(entries);
                }
                logger.finest("Error occurred while putting entries as batch!", t);
                if (error == null) {
                    error = t;
                }
            }
        }

        if (statisticsEnabled) {
            statistics.addPutTimeNanos(System.nanoTime() - start);
        }

        if (error != null) {
            /*
             * There maybe multiple exceptions but we throw only the first one.
             * There are some ideas to throw all exceptions to caller but all of them have drawbacks:
             *      - `Thread::addSuppressed` can be used to add other exceptions to the first one
             *        but it is available since JDK 7.
             *      - `Thread::initCause` can be used but this is wrong as semantic
             *        since the other exceptions are not cause of the first one.
             *      - We may wrap all exceptions in our custom exception (such as `MultipleCacheException`)
             *        but in this case caller may wait different exception type and this idea causes problem.
             *        For example see this TCK test:
             *              `org.jsr107.tck.integration.CacheWriterTest::shouldWriteThoughUsingPutAll_partialSuccess`
             *        In this test exception is thrown at `CacheWriter` and caller side expects this exception.
             * So as a result, we only throw the first exception and others are suppressed by only logging.
             */
            rethrow(error);
        }
    }

    private void handleNearCacheOnPutAll(List<Map.Entry<Data, Data>> entries) {
        if (nearCacheEnabled) {
            if (cacheOnUpdate) {
                for (Map.Entry<Data, Data> entry : entries) {
                    storeInNearCache(entry.getKey(), entry.getValue(), null);
                }
            } else {
                for (Map.Entry<Data, Data> entry : entries) {
                    invalidateNearCache(entry.getKey());
                }
            }
        }
    }

    private static final class ClientNearCacheExecutor implements NearCacheExecutor {

        private ClientExecutionService clientExecutionService;

        private ClientNearCacheExecutor(ClientExecutionService clientExecutionService) {
            this.clientExecutionService = clientExecutionService;
        }

        @Override
        public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return clientExecutionService.scheduleWithRepetition(command, initialDelay, delay, unit);
        }
    }

    private NearCacheExecutor createNearCacheExecutor(ClientExecutionService clientExecutionService) {
        return new ClientNearCacheExecutor(clientExecutionService);
    }

    @Override
    public void close() {
        if (nearCacheEnabled) {
            removeInvalidationListener();
            nearCacheManager.clearNearCache(nearCache.getName());
        }
        if (statisticsEnabled) {
            statistics.clear();
        }
        super.close();
    }

    @Override
    protected void onDestroy() {
        if (nearCacheEnabled) {
            removeInvalidationListener();
            nearCacheManager.destroyNearCache(nearCache.getName());
        }
        if (statisticsEnabled) {
            statistics.clear();
        }
    }

    protected ClientInvocationFuture invoke(ClientMessage req, int partitionId, int completionId) {
        final boolean completionOperation = completionId != -1;
        if (completionOperation) {
            registerCompletionLatch(completionId, 1);
        }
        try {
            HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
            final ClientInvocation clientInvocation = new ClientInvocation(client, req, partitionId);
            ClientInvocationFuture f = clientInvocation.invoke();
            if (completionOperation) {
                waitCompletionLatch(completionId, f);
            }
            return f;
        } catch (Throwable e) {
            if (e instanceof IllegalStateException) {
                close();
            }
            if (completionOperation) {
                deregisterCompletionLatch(completionId);
            }
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected ClientInvocationFuture invoke(ClientMessage req, Data keyData, int completionId) {
        int partitionId = clientContext.getPartitionService().getPartitionId(keyData);
        return invoke(req, partitionId, completionId);
    }

    protected <T> T getSafely(Future<T> future) {
        try {
            return future.get();
        } catch (Throwable throwable) {
            throw rethrow(throwable);
        }
    }

    protected ICompletableFuture<V> getAndRemoveAsyncInternal(K key, boolean withCompletionEvent) {
        final long start = System.nanoTime();
        ensureOpen();
        validateNotNull(key);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key);
        final Data keyData = toData(key);
        final int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheGetAndRemoveCodec.encodeRequest(nameWithPrefix, keyData, completionId);
        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, completionId);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw rethrow(e);
        }
        if (statisticsEnabled) {
            return new CacheMaintenanceClientDelegatingFuture<V>(future, getSerializationService(),
                    GET_AND_REMOVE_RESPONSE_DECODER, nearCacheEnabled, statisticsEnabled) {
                @Override
                public void handleNearCacheUpdate() {
                }

                @Override
                public void handleStatistics() {
                    handleStatisticsOnRemove(true, start, deserializedValue);
                }
            };
        } else {
            return new ClientDelegatingFuture<V>(future, getSerializationService(), GET_AND_REMOVE_RESPONSE_DECODER);
        }
    }

    protected ICompletableFuture<Boolean> removeAsyncInternal(K key, V oldValue, boolean hasOldValue,
                                                              boolean withCompletionEvent) {
        final long start = System.nanoTime();
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue);
        } else {
            validateNotNull(key);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key);
        }
        final Data keyData = toData(key);
        final Data oldValueData = toData(oldValue);
        final int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheRemoveCodec.encodeRequest(nameWithPrefix, keyData, oldValueData, completionId);
        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, completionId);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw rethrow(e);
        }
        if (statisticsEnabled) {
            return new CacheMaintenanceClientDelegatingFuture<Boolean>(future, getSerializationService(), REMOVE_RESPONSE_DECODER,
                    nearCacheEnabled, statisticsEnabled) {
                @Override
                public void handleNearCacheUpdate() {
                }

                @Override
                public void handleStatistics() {
                    handleStatisticsOnRemove(false, start, deserializedValue);
                }
            };
        } else {
            return new ClientDelegatingFuture<Boolean>(future, getSerializationService(), REMOVE_RESPONSE_DECODER);
        }
    }

    protected void handleStatisticsOnRemove(boolean isGet, long start, Object response) {
        if (isGet) {
            statistics.addGetTimeNanos(System.nanoTime() - start);
            if (response != null) {
                statistics.increaseCacheHits();
                statistics.increaseCacheRemovals();
                statistics.addRemoveTimeNanos(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses();
            }
        } else {
            if (TRUE.equals(response)) {
                statistics.increaseCacheRemovals();
                statistics.addRemoveTimeNanos(System.nanoTime() - start);
            }
        }
    }

    protected ICompletableFuture<Boolean> replaceAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                               boolean hasOldValue, boolean withCompletionEvent) {
        final long start = System.nanoTime();
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, newValue);
        }

        final Data keyData = toData(key);
        final Data oldValueData = toData(oldValue);
        final Data newValueData = toData(newValue);
        final Data expiryPolicyData = toData(expiryPolicy);
        final int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheReplaceCodec.encodeRequest(nameWithPrefix, keyData, oldValueData, newValueData,
                expiryPolicyData, completionId);
        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, completionId);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw rethrow(e);
        }

        if (statisticsEnabled) {
            return new CacheMaintenanceClientDelegatingFuture<Boolean>(future, getSerializationService(),
                    REPLACE_RESPONSE_DECODER, nearCacheEnabled, statisticsEnabled) {
                @Override
                public void handleNearCacheUpdate() {
                }

                @Override
                public void handleStatistics() {
                    handleStatisticsOnReplace(false, start, deserializedValue);
                }
            };
        } else {
            return new ClientDelegatingFuture<Boolean>(future, getSerializationService(), REPLACE_RESPONSE_DECODER);
        }
    }

    protected ICompletableFuture<V> replaceAndGetAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                                   boolean hasOldValue, boolean withCompletionEvent) {
        final long start = System.nanoTime();
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, newValue);
        }

        final Data keyData = toData(key);
        final Data newValueData = toData(newValue);
        final Data expiryPolicyData = toData(expiryPolicy);
        final int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheGetAndReplaceCodec.encodeRequest(nameWithPrefix, keyData, newValueData, expiryPolicyData,
                completionId);
        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, completionId);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw rethrow(e);
        }

        if (statisticsEnabled) {
            return new CacheMaintenanceClientDelegatingFuture<V>(future, getSerializationService(),
                    GET_AND_REPLACE_RESPONSE_DECODER, nearCacheEnabled, statisticsEnabled) {
                @Override
                public void handleNearCacheUpdate() {
                }

                @Override
                public void handleStatistics() {
                    handleStatisticsOnReplace(true, start, deserializedValue);
                }
            };
        } else {
            return new ClientDelegatingFuture<V>(future, getSerializationService(), GET_AND_REPLACE_RESPONSE_DECODER);
        }
    }

    protected void handleStatisticsOnReplace(boolean isGet, long start, Object response) {
        if (isGet) {
            statistics.addGetTimeNanos(System.nanoTime() - start);
            if (response != null) {
                statistics.increaseCacheHits();
                statistics.increaseCachePuts();
                statistics.addPutTimeNanos(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses();
            }
        } else {
            if (TRUE.equals(response)) {
                statistics.increaseCacheHits();
                statistics.increaseCachePuts();
                statistics.addPutTimeNanos(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses();
            }
        }
    }

    // if {@code isGet==true} then {@code future.get} will return the previous value associated with {@code key}, otherwise
    // {@code null} will be returned.
    protected ClientDelegatingFuture<V> putAsyncInternal(K key, final V value, ExpiryPolicy expiryPolicy, final boolean isGet,
                                                         boolean withCompletionEvent) {
        final long start = System.nanoTime();
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);

        final Data keyData = toData(key);
        final Data valueData = toData(value);
        Data expiryPolicyData = toData(expiryPolicy);
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CachePutCodec.encodeRequest(nameWithPrefix, keyData, valueData, expiryPolicyData, isGet,
                completionId);
        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, completionId);
        } catch (Exception e) {
            throw rethrow(e);
        }

        if (isNearCacheOrStatsEnabled()) {
            return new CacheMaintenanceClientDelegatingFuture<V>(future, getSerializationService(), PUT_RESPONSE_DECODER,
                    nearCacheEnabled, statisticsEnabled) {
                @Override
                public void handleNearCacheUpdate() {
                    if (cacheOnUpdate) {
                        storeInNearCache(keyData, valueData, value);
                    } else {
                        invalidateNearCache(keyData);
                    }
                }

                @Override
                public void handleStatistics() {
                    handleStatisticsOnPut(isGet, start, deserializedValue);
                }
            };
        } else {
            return new ClientDelegatingFuture(future, getSerializationService(), PUT_RESPONSE_DECODER);
        }
    }

    protected void handleStatisticsOnPut(boolean isGet, long start, Object response) {
        statistics.increaseCachePuts();
        statistics.addPutTimeNanos(System.nanoTime() - start);
        if (isGet) {
            Object resp = getSerializationService().toObject(response);
            statistics.addGetTimeNanos(System.nanoTime() - start);
            if (resp == null) {
                statistics.increaseCacheMisses();
            } else {
                statistics.increaseCacheHits();
            }
        }
    }

    protected ClientDelegatingFuture<Boolean> putIfAbsentAsyncInternal(K key, final V value, ExpiryPolicy expiryPolicy,
                                                                 boolean withCompletionEvent) {
        final long start = System.nanoTime();
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);

        final Data keyData = toData(key);
        final Data valueData = toData(value);
        Data expiryPolicyData = toData(expiryPolicy);
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CachePutIfAbsentCodec.encodeRequest(nameWithPrefix, keyData, valueData,
                expiryPolicyData, completionId);
        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, completionId);
        } catch (Exception e) {
            throw rethrow(e);
        }
        if (isNearCacheOrStatsEnabled()) {
            return new CacheMaintenanceClientDelegatingFuture<Boolean>(future, getSerializationService(),
                    PUT_IF_ABSENT_RESPONSE_DECODER, nearCacheEnabled, statisticsEnabled) {
                @Override
                public void handleNearCacheUpdate() {
                    if (cacheOnUpdate) {
                        storeInNearCache(keyData, valueData, value);
                    } else {
                        invalidateNearCache(keyData);
                    }
                }

                @Override
                public void handleStatistics() {
                    handleStatisticsOnPutIfAbsent(start, deserializedValue);
                }
            };
        } else {
            return new ClientDelegatingFuture<Boolean>(future, getSerializationService(), PUT_IF_ABSENT_RESPONSE_DECODER);
        }
    }

    protected void handleStatisticsOnPutIfAbsent(long start, boolean saved) {
        if (saved) {
            statistics.increaseCachePuts();
            statistics.addPutTimeNanos(System.nanoTime() - start);
        }
    }

    protected void removeAllKeysInternal(Set<? extends K> keys) {
        final long start = System.nanoTime();
        final Set<Data> keysData;
        keysData = new HashSet<Data>();
        for (K key : keys) {
            keysData.add(toData(key));
        }
        final int partitionCount = clientContext.getPartitionService().getPartitionCount();
        final int completionId = nextCompletionId();
        registerCompletionLatch(completionId, partitionCount);
        ClientMessage request = CacheRemoveAllKeysCodec.encodeRequest(nameWithPrefix, keysData, completionId);
        try {
            invoke(request);
            waitCompletionLatch(completionId, null);
            if (statisticsEnabled) {
                // Actually we don't know how many of them are really removed or not.
                // We just assume that if there is no exception, all of them are removed.
                // Otherwise (if there is an exception), we don't update any cache stats about remove.
                statistics.increaseCacheRemovals(keysData.size());
                statistics.addRemoveTimeNanos(System.nanoTime() - start);
            }
        } catch (Throwable t) {
            deregisterCompletionLatch(completionId);
            throw rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    protected void removeAllInternal() {
        final int partitionCount = clientContext.getPartitionService().getPartitionCount();
        final int completionId = nextCompletionId();
        registerCompletionLatch(completionId, partitionCount);
        ClientMessage request = CacheRemoveAllCodec.encodeRequest(nameWithPrefix, completionId);
        try {
            invoke(request);
            waitCompletionLatch(completionId, null);
            if (statisticsEnabled) {
                statistics.setLastUpdateTime(System.currentTimeMillis());
                // We don't support count stats of removing all entries.
            }
        } catch (Throwable t) {
            deregisterCompletionLatch(completionId);
            throw rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    protected void clearInternal() {
        ClientMessage request = CacheClearCodec.encodeRequest(nameWithPrefix);
        try {
            invoke(request);
            if (statisticsEnabled) {
                statistics.setLastUpdateTime(System.currentTimeMillis());
                // We don't support count stats of removing all entries.
            }
        } catch (Throwable t) {
            throw rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    /**
     * Store a value in near-cache, if one is configured.
     * @param key
     * @param valueData
     * @param value
     */
    protected void storeInNearCache(Data key, Data valueData, V value) {
        if (nearCacheEnabled && valueData != null) {
            Object valueToStore = nearCache.selectToSave(value, valueData);
            nearCache.put(key, valueToStore);
        }
    }

    protected void invalidateNearCache(Data key) {
        if (nearCacheEnabled) {
            nearCache.remove(key);
        }
    }

    protected void addListenerLocally(String regId, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        if (cacheEntryListenerConfiguration.isSynchronous()) {
            syncListenerRegistrations.putIfAbsent(cacheEntryListenerConfiguration, regId);
        } else {
            asyncListenerRegistrations.putIfAbsent(cacheEntryListenerConfiguration, regId);
        }
    }

    protected String removeListenerLocally(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        final ConcurrentMap<CacheEntryListenerConfiguration, String> regs;
        if (cacheEntryListenerConfiguration.isSynchronous()) {
            regs = syncListenerRegistrations;
        } else {
            regs = asyncListenerRegistrations;
        }
        return regs.remove(cacheEntryListenerConfiguration);
    }

    protected String getListenerIdLocal(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        final ConcurrentMap<CacheEntryListenerConfiguration, String> regs;
        if (cacheEntryListenerConfiguration.isSynchronous()) {
            regs = syncListenerRegistrations;
        } else {
            regs = asyncListenerRegistrations;
        }
        return regs.get(cacheEntryListenerConfiguration);
    }

    private void deregisterAllCacheEntryListener(Collection<String> listenerRegistrations) {
        ClientListenerService listenerService = clientContext.getListenerService();
        for (String regId : listenerRegistrations) {
            listenerService.deregisterListener(regId);
        }
    }

    @Override
    protected void closeListeners() {
        deregisterAllCacheEntryListener(syncListenerRegistrations.values());
        deregisterAllCacheEntryListener(asyncListenerRegistrations.values());

        syncListenerRegistrations.clear();
        asyncListenerRegistrations.clear();
        notifyAndClearSyncListenerLatches();
    }

    private void notifyAndClearSyncListenerLatches() {
        // notify waiting sync listeners
        Collection<CountDownLatch> latches = syncLocks.values();
        Iterator<CountDownLatch> iterator = latches.iterator();
        while (iterator.hasNext()) {
            CountDownLatch latch = iterator.next();
            iterator.remove();
            while (latch.getCount() > 0) {
                latch.countDown();
            }
        }
    }

    public void countDownCompletionLatch(int countDownLatchId) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
            if (countDownLatch == null) {
                return;
            }
            countDownLatch.countDown();
            if (countDownLatch.getCount() == 0) {
                deregisterCompletionLatch(countDownLatchId);
            }
        }
    }

    protected Integer registerCompletionLatch(Integer countDownLatchId, int count) {
        if (!syncListenerRegistrations.isEmpty()) {
            int size = syncListenerRegistrations.size();
            CountDownLatch countDownLatch = new CountDownLatch(count * size);
            syncLocks.put(countDownLatchId, countDownLatch);
            return countDownLatchId;
        }
        return MutableOperation.IGNORE_COMPLETION;
    }

    protected void deregisterCompletionLatch(Integer countDownLatchId) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            syncLocks.remove(countDownLatchId);
        }
    }

    protected void waitCompletionLatch(Integer countDownLatchId, ICompletableFuture future)
            throws ExecutionException {
        if (countDownLatchId != IGNORE_COMPLETION) {
            final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
            if (countDownLatch != null) {
                awaitLatch(countDownLatch, future);
            }
        }
    }

    private void awaitLatch(CountDownLatch countDownLatch, ICompletableFuture future)
            throws ExecutionException {
        try {
            long currentTimeoutMs = MAX_COMPLETION_LATCH_WAIT_TIME;
            // Call latch await in small steps to be able to check if node is still active.
            // If not active then throw HazelcastInstanceNotActiveException,
            // If closed or destroyed then throw IllegalStateException,
            // otherwise continue to wait until `MAX_COMPLETION_LATCH_WAIT_TIME` passes.
            //
            // Warning: Silently ignoring if latch does not countDown in time.
            while (currentTimeoutMs > 0
                    && !countDownLatch.await(COMPLETION_LATCH_WAIT_TIME_STEP, TimeUnit.MILLISECONDS)) {
                if (future != null && future.isDone()) {
                    Object response = future.get();
                    if (response instanceof Throwable) {
                        return;
                    }
                }
                currentTimeoutMs -= COMPLETION_LATCH_WAIT_TIME_STEP;
                if (!clientContext.isActive()) {
                    throw new HazelcastInstanceNotActiveException();
                } else if (isClosed()) {
                    throw new IllegalStateException("Cache (" + nameWithPrefix + ") is closed !");
                } else if (isDestroyed()) {
                    throw new IllegalStateException("Cache (" + nameWithPrefix + ") is destroyed !");
                }
            }
            if (countDownLatch.getCount() > 0) {
                logger.finest("Countdown latch wait timeout after "
                        + MAX_COMPLETION_LATCH_WAIT_TIME + " milliseconds!");
            }
        } catch (InterruptedException e) {
            ExceptionUtil.sneakyThrow(e);
        }
    }

    protected EventHandler createHandler(CacheEventListenerAdaptor<K, V> adaptor) {
        return new CacheEventHandler(adaptor);
    }

    private static final class FutureEntriesTuple {

        private Future future;
        private List<Map.Entry<Data, Data>> entries;

        private FutureEntriesTuple(Future future, List<Map.Entry<Data, Data>> entries) {
            this.future = future;
            this.entries = entries;
        }

    }

    private final class CacheEventHandler
            extends CacheAddEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final CacheEventListenerAdaptor<K, V> adaptor;

        private CacheEventHandler(CacheEventListenerAdaptor<K, V> adaptor) {
            this.adaptor = adaptor;
        }

        @Override
        public void handle(int type, Collection<CacheEventData> keys, int completionId) {
            adaptor.handle(type, keys, completionId);
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }

    protected ICompletableFuture createCompletedFuture(Object value) {
        return new CompletedFuture(getSerializationService(), value,
                clientContext.getExecutionService().getAsyncExecutor());
    }

    private final class NearCacheInvalidationHandler
            extends CacheAddInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final Client client;

        private NearCacheInvalidationHandler(Client client) {
            this.client = client;
        }

        @Override
        public void handle(String name, Data key, String sourceUuid) {
            if (client.getUuid().equals(sourceUuid)) {
                return;
            }
            if (key != null) {
                nearCache.remove(key);
            } else {
                nearCache.clear();
            }
        }

        @Override
        public void handle(String name, Collection<Data> keys, Collection<String> sourceUuids) {
            if (sourceUuids != null && !sourceUuids.isEmpty()) {
                Iterator<Data> keysIt = keys.iterator();
                Iterator<String> sourceUuidsIt = sourceUuids.iterator();
                while (keysIt.hasNext() && sourceUuidsIt.hasNext()) {
                    Data key = keysIt.next();
                    String sourceUuid = sourceUuidsIt.next();
                    if (!client.getUuid().equals(sourceUuid)) {
                        nearCache.remove(key);
                    }
                }
            } else {
                for (Data key : keys) {
                    nearCache.remove(key);
                }
            }
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
            nearCache.clear();
        }
    }

    private void registerInvalidationListener() {
        if (nearCacheEnabled && nearCache.isInvalidateOnChange()) {
            Client client = clientContext.getClusterService().getLocalClient();
            EventHandler handler = new NearCacheInvalidationHandler(client);
            ListenerMessageCodec listenerCodec = createInvalidationListenerCodec();
            nearCacheMembershipRegistrationId = clientContext.getListenerService().registerListener(listenerCodec, handler);
        }
    }

    private ListenerMessageCodec createInvalidationListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return CacheAddInvalidationListenerCodec.encodeRequest(nameWithPrefix, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return CacheAddInvalidationListenerCodec.decodeResponse(clientMessage).response;
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

    private void removeInvalidationListener() {
        if (nearCacheEnabled && nearCache.isInvalidateOnChange()) {
            String registrationId = nearCacheMembershipRegistrationId;
            if (registrationId != null) {
                clientContext.getListenerService().deregisterListener(registrationId);
            }
        }
    }

    private boolean isNearCacheOrStatsEnabled() {
        return nearCacheEnabled || statisticsEnabled;
    }
}
