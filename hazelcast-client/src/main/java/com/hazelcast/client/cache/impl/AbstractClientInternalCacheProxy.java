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
import com.hazelcast.client.impl.protocol.codec.CacheGetAndRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAndReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllKeysCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheReplaceCodec;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.CompletedFuture;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
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

/**
 * Abstract {@link com.hazelcast.cache.ICache} implementation which provides shared internal implementations
 * of cache operations like put, replace, remove and invoke. These internal implementations are delegated
 * by actual cache methods.
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

    private static final long MAX_COMPLETION_LATCH_WAIT_TIME = TimeUnit.MINUTES.toMillis(5);
    private static final long COMPLETION_LATCH_WAIT_TIME_STEP = TimeUnit.SECONDS.toMillis(1);

    private static final ClientMessageDecoder GET_AND_REMOVE_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheGetAndRemoveCodec.decodeResponse(clientMessage).response;
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

    private void initNearCache() {
        NearCacheConfig nearCacheConfig = clientContext.getClientConfig().getNearCacheConfig(name);
        if (nearCacheConfig != null) {
            cacheOnUpdate = nearCacheConfig.getLocalUpdatePolicy() == NearCacheConfig.LocalUpdatePolicy.CACHE;
            NearCacheContext nearCacheContext =
                    new NearCacheContext(nearCacheManager,
                            clientContext.getSerializationService(),
                            createNearCacheExecutor(clientContext.getExecutionService()));
            nearCache = nearCacheManager.getOrCreateNearCache(nameWithPrefix, nearCacheConfig, nearCacheContext);
            registerInvalidationListener();
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
        if (nearCache != null) {
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
        if (nearCache != null) {
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

    protected <T> ICompletableFuture<T> getAndRemoveAsyncInternal(K key, boolean withCompletionEvent, boolean async) {
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
        ClientDelegatingFuture<T> delegatingFuture =
                new ClientDelegatingFuture<T>(future, clientContext.getSerializationService(), GET_AND_REMOVE_RESPONSE_DECODER);
        if (async && statisticsEnabled) {
            delegatingFuture.andThen(new ExecutionCallback<T>() {
                public void onResponse(T responseData) {
                    Object response = clientContext.getSerializationService().toObject(responseData);
                    handleStatisticsOnRemove(true, start, response);
                }

                public void onFailure(Throwable t) {

                }
            });
        }
        return delegatingFuture;
    }

    protected <T> ICompletableFuture<T> removeAsyncInternal(K key, V oldValue, boolean hasOldValue, boolean withCompletionEvent,
                                                            boolean async) {
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
        ClientDelegatingFuture<T> delegatingFuture =
                new ClientDelegatingFuture<T>(future, clientContext.getSerializationService(), REMOVE_RESPONSE_DECODER);
        if (async && statisticsEnabled) {
            delegatingFuture.andThen(new ExecutionCallback<T>() {
                public void onResponse(T responseData) {
                    Object response = clientContext.getSerializationService().toObject(responseData);
                    handleStatisticsOnRemove(false, start, response);
                }

                public void onFailure(Throwable t) {
                }
            });
        }
        return delegatingFuture;
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
            if (Boolean.TRUE.equals(response)) {
                statistics.increaseCacheRemovals();
                statistics.addRemoveTimeNanos(System.nanoTime() - start);
            }
        }
    }

    protected <T> ICompletableFuture<T> replaceInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                        boolean hasOldValue, boolean withCompletionEvent,
                                                        boolean async) {
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

        ClientDelegatingFuture<T> delegatingFuture =
                new ClientDelegatingFuture<T>(future, clientContext.getSerializationService(), REPLACE_RESPONSE_DECODER);
        if (async && statisticsEnabled) {
            delegatingFuture.andThen(new ExecutionCallback<T>() {
                public void onResponse(T responseData) {
                    Object response = clientContext.getSerializationService().toObject(responseData);
                    handleStatisticsOnReplace(false, start, response);
                }

                public void onFailure(Throwable t) {
                }
            });
        }
        return delegatingFuture;
    }

    protected <T> ICompletableFuture<T> replaceAndGetAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                                   boolean hasOldValue, boolean withCompletionEvent,
                                                                   boolean async) {
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

        ClientDelegatingFuture<T> delegatingFuture =
                new ClientDelegatingFuture<T>(future, clientContext.getSerializationService(), GET_AND_REPLACE_RESPONSE_DECODER);
        if (async && statisticsEnabled) {
            delegatingFuture.andThen(new ExecutionCallback<T>() {
                public void onResponse(T responseData) {
                    Object response = clientContext.getSerializationService().toObject(responseData);
                    handleStatisticsOnReplace(true, start, response);
                }

                public void onFailure(Throwable t) {
                }
            });
        }
        return delegatingFuture;
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
            if (Boolean.TRUE.equals(response)) {
                statistics.increaseCacheHits();
                statistics.increaseCachePuts();
                statistics.addPutTimeNanos(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses();
            }
        }
    }

    protected Object putInternal(K key, final V value, ExpiryPolicy expiryPolicy, boolean isGet, boolean withCompletionEvent,
                                 boolean async) {
        long start = System.nanoTime();
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);

        Data keyData = toData(key);
        Data valueData = toData(value);
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

        if (async) {
            return putInternalAsync(value, isGet, start, keyData, valueData, future);
        }
        return putInternalSync(value, isGet, start, keyData, valueData, future);
    }

    private Object putInternalAsync(final V value, final boolean isGet, final long start, final Data keyData,
                                    final Data valueData, ClientInvocationFuture future) {
        OneShotExecutionCallback oneShotExecutionCallback = null;
        if (nearCache != null || statisticsEnabled) {
            oneShotExecutionCallback = new OneShotExecutionCallback() {
                @Override
                protected void onResponseInternal(Object responseData) {
                    if (nearCache != null) {
                        if (cacheOnUpdate) {
                            storeInNearCache(keyData, valueData, value);
                        } else {
                            invalidateNearCache(keyData);
                        }
                    }
                    if (statisticsEnabled) {
                        handleStatisticsOnPut(isGet, start, responseData);
                    }
                }

                @Override
                protected void onFailureInternal(Throwable t) {
                }
            };
        }
        ClientDelegatingFuture delegatingFuture;
        if (oneShotExecutionCallback != null) {
            delegatingFuture =
                    new CallbackAwareClientDelegatingFuture(future, clientContext.getSerializationService(),
                            PUT_RESPONSE_DECODER, oneShotExecutionCallback);
            delegatingFuture.andThen(oneShotExecutionCallback);
        } else {
            delegatingFuture =
                    new ClientDelegatingFuture(future, clientContext.getSerializationService(), PUT_RESPONSE_DECODER);
        }
        return delegatingFuture;
    }

    private Object putInternalSync(V value, boolean isGet, long start, Data keyData, Data valueData,
                                   ClientInvocationFuture future) {
        try {
            ClientDelegatingFuture delegatingFuture = new ClientDelegatingFuture(future, clientContext.getSerializationService(),
                    PUT_RESPONSE_DECODER);
            Object response = delegatingFuture.get();
            if (nearCache != null) {
                if (cacheOnUpdate) {
                    storeInNearCache(keyData, valueData, value);
                } else {
                    invalidateNearCache(keyData);
                }
            }
            if (statisticsEnabled) {
                handleStatisticsOnPut(isGet, start, response);
            }
            return response;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected void handleStatisticsOnPut(boolean isGet, long start, Object response) {
        statistics.increaseCachePuts();
        statistics.addPutTimeNanos(System.nanoTime() - start);
        if (isGet) {
            Object resp = clientContext.getSerializationService().toObject(response);
            statistics.addGetTimeNanos(System.nanoTime() - start);
            if (resp == null) {
                statistics.increaseCacheMisses();
            } else {
                statistics.increaseCacheHits();
            }
        }
    }

    protected Object putIfAbsentInternal(K key, V value, ExpiryPolicy expiryPolicy, boolean withCompletionEvent, boolean async) {
        long start = System.nanoTime();
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);

        Data keyData = toData(key);
        Data valueData = toData(value);
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
        ClientDelegatingFuture<Boolean> delegatingFuture = new ClientDelegatingFuture<Boolean>(future,
                clientContext.getSerializationService(), PUT_IF_ABSENT_RESPONSE_DECODER);

        if (async) {
            return putIfAbsentInternalAsync(value, start, keyData, valueData, delegatingFuture);
        }
        return putIfAbsentInternalSync(value, start, keyData, valueData, delegatingFuture);
    }

    private Object putIfAbsentInternalAsync(final V value, final long start, final Data keyData, final Data valueData,
                                            ClientDelegatingFuture<Boolean> delegatingFuture) {
        if (nearCache != null || statisticsEnabled) {
            delegatingFuture.andThen(new ExecutionCallback<Boolean>() {
                @Override
                public void onResponse(Boolean responseData) {
                    if (nearCache != null) {
                        if (cacheOnUpdate) {
                            storeInNearCache(keyData, valueData, value);
                        } else {
                            invalidateNearCache(keyData);
                        }
                    }
                    if (statisticsEnabled) {
                        Object response = clientContext.getSerializationService().toObject(responseData);
                        handleStatisticsOnPutIfAbsent(start, (Boolean) response);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                }
            });
        }
        return delegatingFuture;
    }

    private Object putIfAbsentInternalSync(V value, long start, Data keyData, Data valueData,
                                           ClientDelegatingFuture<Boolean> delegatingFuture) {
        try {
            Object response = delegatingFuture.get();
            if (nearCache != null) {
                if (cacheOnUpdate) {
                    storeInNearCache(keyData, valueData, value);
                } else {
                    invalidateNearCache(keyData);
                }
            }
            if (statisticsEnabled) {
                handleStatisticsOnPutIfAbsent(start, (Boolean) response);
            }
            return response;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
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

    protected void storeInNearCache(Data key, Data valueData, V value) {
        if (nearCache != null && valueData != null) {
            Object valueToStore = nearCache.selectToSave(value, valueData);
            nearCache.put(key, valueToStore);
        }
    }

    protected void invalidateNearCache(Data key) {
        if (nearCache != null) {
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
        return new CompletedFuture(clientContext.getSerializationService(), value,
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
        if (nearCache != null && nearCache.isInvalidateOnChange()) {
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
        if (nearCache != null && nearCache.isInvalidateOnChange()) {
            String registrationId = nearCacheMembershipRegistrationId;
            if (registrationId != null) {
                clientContext.getListenerService().deregisterListener(registrationId);
            }
        }
    }

}
