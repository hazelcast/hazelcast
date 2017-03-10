/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.CacheSyncListenerCompleter;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
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
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.instance.BuildInfo.UNKNOWN_HAZELCAST_VERSION;
import static com.hazelcast.instance.BuildInfo.calculateVersion;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.ExceptionUtil.rethrowAllowedTypeFirst;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.String.format;

/**
 * Abstract {@link com.hazelcast.cache.ICache} implementation which provides shared internal implementations
 * of cache operations like put, replace, remove and invoke. These internal implementations are delegated
 * by actual cache methods.
 * <p>
 * Note: this partial implementation is used by client.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity", "checkstyle:anoninnerlength"})
abstract class AbstractClientInternalCacheProxy<K, V> extends AbstractClientCacheProxyBase<K, V>
        implements CacheSyncListenerCompleter {

    private static final long MAX_COMPLETION_LATCH_WAIT_TIME = TimeUnit.MINUTES.toMillis(5);
    private static final long COMPLETION_LATCH_WAIT_TIME_STEP = TimeUnit.SECONDS.toMillis(1);

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder GET_AND_REMOVE_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheGetAndRemoveCodec.decodeResponse(clientMessage).response;
        }
    };

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder REMOVE_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) Boolean.valueOf(CacheRemoveCodec.decodeResponse(clientMessage).response);
        }
    };

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder REPLACE_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheReplaceCodec.decodeResponse(clientMessage).response;
        }
    };

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder GET_AND_REPLACE_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheGetAndReplaceCodec.decodeResponse(clientMessage).response;
        }
    };

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder PUT_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CachePutCodec.decodeResponse(clientMessage).response;
        }
    };

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder PUT_IF_ABSENT_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) Boolean.valueOf(CachePutIfAbsentCodec.decodeResponse(clientMessage).response);
        }
    };

    protected HazelcastClientCacheManager cacheManager;
    protected NearCacheManager nearCacheManager;
    // Object => Data or <V>
    protected NearCache<Object, Object> nearCache;
    /**
     * used when this cache has no near cache configured
     */

    protected String nearCacheMembershipRegistrationId;
    protected ClientCacheStatisticsImpl statistics;

    protected boolean statisticsEnabled;
    protected boolean cacheOnUpdate;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations;
    private final ConcurrentMap<Integer, CountDownLatch> syncLocks;
    // Eventually consistent near cache can only be used with server versions >= 3.8.
    private final int minConsistentNearCacheSupportingServerVersion = calculateVersion("3.8");

    protected AbstractClientInternalCacheProxy(CacheConfig<K, V> cacheConfig) {
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
            statistics = new ClientCacheStatisticsImpl(System.currentTimeMillis(), nearCache.getNearCacheStats());
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

    void initNearCache() {
        ClientConfig clientConfig = clientContext.getClientConfig();
        NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig(name);
        if (nearCacheConfig != null) {
            checkNearCacheConfig(nearCacheConfig, clientConfig.getNativeMemoryConfig());
            cacheOnUpdate = isCacheOnUpdate(nearCacheConfig, nameWithPrefix, logger);
            ICacheDataStructureAdapter<K, V> adapter = new ICacheDataStructureAdapter<K, V>(this);
            nearCache = nearCacheManager.getOrCreateNearCache(nameWithPrefix, nearCacheConfig, adapter);
            registerInvalidationListener();
        }
    }

    private static void checkNearCacheConfig(NearCacheConfig nearCacheConfig, NativeMemoryConfig nativeMemoryConfig) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat != NATIVE) {
            return;
        }

        checkTrue(nativeMemoryConfig.isEnabled(), "Enable native memory config to use NATIVE in-memory-format for Near Cache");

    }

    static boolean isCacheOnUpdate(NearCacheConfig nearCacheConfig, String cacheName, ILogger logger) {
        NearCacheConfig.LocalUpdatePolicy localUpdatePolicy = nearCacheConfig.getLocalUpdatePolicy();
        if (localUpdatePolicy == CACHE) {
            logger.warning(format("Deprecated local update policy is found for cache `%s`."
                            + " The policy `%s` is subject to remove in further releases. Instead you can use `%s`",
                    cacheName, CACHE, CACHE_ON_UPDATE));
            return true;
        }

        return localUpdatePolicy == CACHE_ON_UPDATE;
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
        boolean completionOperation = completionId != -1;
        if (completionOperation) {
            registerCompletionLatch(completionId, 1);
        }
        try {
            HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
            ClientInvocation clientInvocation = new ClientInvocation(client, req, partitionId);
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
            delegatingFuture.andThenInternal(new ExecutionCallback<T>() {
                public void onResponse(T response) {
                    handleStatisticsOnRemove(true, start, response);
                }

                public void onFailure(Throwable t) {

                }
            }, true);
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
        Data keyData = toData(key);
        Data oldValueData = toData(oldValue);
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
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
            delegatingFuture.andThenInternal(new ExecutionCallback<T>() {
                public void onResponse(T response) {
                    handleStatisticsOnRemove(false, start, response);
                }

                public void onFailure(Throwable t) {
                }
            }, true);
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
                                                        boolean hasOldValue, boolean withCompletionEvent, boolean async) {
        final long start = System.nanoTime();
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, newValue);
        }

        Data keyData = toData(key);
        Data oldValueData = toData(oldValue);
        Data newValueData = toData(newValue);
        Data expiryPolicyData = toData(expiryPolicy);
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
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
            delegatingFuture.andThenInternal(new ExecutionCallback<T>() {
                public void onResponse(T response) {
                    handleStatisticsOnReplace(false, start, response);
                }

                public void onFailure(Throwable t) {
                }
            }, true);
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

        Data keyData = toData(key);
        Data newValueData = toData(newValue);
        Data expiryPolicyData = toData(expiryPolicy);
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
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
            delegatingFuture.andThenInternal(new ExecutionCallback<T>() {
                public void onResponse(T response) {
                    handleStatisticsOnReplace(true, start, response);
                }

                public void onFailure(Throwable t) {
                }
            }, true);
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

    protected Object putInternal(K key, V value, ExpiryPolicy expiryPolicy, boolean isGet, boolean withCompletionEvent,
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

    protected long tryReserveForUpdate(Data keyData) {
        if (nearCache == null) {
            return NOT_RESERVED;
        }
        return nearCache.tryReserveForUpdate(keyData);
    }

    protected void releaseRemainingReservedKeys(Map<Data, Long> reservedKeys) {
        if (nearCache == null) {
            return;
        }

        for (Data key : reservedKeys.keySet()) {
            nearCache.remove(key);
        }
    }

    private Object putInternalAsync(final V value, final boolean isGet, final long start, final Data keyData,
                                    final Data valueData, ClientInvocationFuture future) {
        OneShotExecutionCallback<V> oneShotExecutionCallback = null;
        if (nearCache != null || statisticsEnabled) {
            oneShotExecutionCallback = new OneShotExecutionCallback<V>() {
                @Override
                protected void onResponseInternal(V responseData) {
                    if (nearCache != null) {
                        if (cacheOnUpdate) {
                            storeInNearCache(keyData, valueData, value, NOT_RESERVED, cacheOnUpdate);
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

        SerializationService serializationService = clientContext.getSerializationService();
        if (oneShotExecutionCallback == null) {
            return new ClientDelegatingFuture<V>(future, serializationService, PUT_RESPONSE_DECODER);
        }
        ClientDelegatingFuture<V> delegatingFuture = new CallbackAwareClientDelegatingFuture<V>(future,
                serializationService, PUT_RESPONSE_DECODER, oneShotExecutionCallback);
        delegatingFuture.andThenInternal(oneShotExecutionCallback, true);
        return delegatingFuture;
    }

    private Object putInternalSync(V value, boolean isGet, long start, Data keyData, Data valueData,
                                   ClientInvocationFuture future) {
        try {
            ClientDelegatingFuture delegatingFuture = new ClientDelegatingFuture(future,
                    clientContext.getSerializationService(), PUT_RESPONSE_DECODER);
            Object response = delegatingFuture.get();

            if (statisticsEnabled) {
                handleStatisticsOnPut(isGet, start, response);
            }
            return response;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        } finally {
            if (nearCache != null) {
                if (cacheOnUpdate) {
                    storeInNearCache(keyData, valueData, value, NOT_RESERVED, cacheOnUpdate);
                } else {
                    invalidateNearCache(keyData);
                }
            }
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

    protected Object putIfAbsentInternal(K key, V value, ExpiryPolicy expiryPolicy, boolean withCompletionEvent,
                                         boolean async) {
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
        } catch (Throwable t) {
            throw rethrow(t);
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
            delegatingFuture.andThenInternal(new ExecutionCallback<Boolean>() {
                @Override
                public void onResponse(Boolean responseData) {
                    if (nearCache != null) {
                        if (cacheOnUpdate) {
                            storeInNearCache(keyData, valueData, value, NOT_RESERVED, cacheOnUpdate);
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
            }, true);
        }
        return delegatingFuture;
    }

    private Object putIfAbsentInternalSync(V value, long start, Data keyData, Data valueData,
                                           ClientDelegatingFuture<Boolean> delegatingFuture) {
        try {
            Object response = delegatingFuture.get();

            if (statisticsEnabled) {
                handleStatisticsOnPutIfAbsent(start, (Boolean) response);
            }
            return response;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        } finally {
            if (nearCache != null) {
                if (cacheOnUpdate) {
                    storeInNearCache(keyData, valueData, value, NOT_RESERVED, cacheOnUpdate);
                } else {
                    invalidateNearCache(keyData);
                }
            }
        }
    }

    protected void handleStatisticsOnPutIfAbsent(long start, boolean saved) {
        if (saved) {
            statistics.increaseCachePuts();
            statistics.addPutTimeNanos(System.nanoTime() - start);
        }
    }

    protected void removeAllKeysInternal(Set<? extends K> keys) {
        long start = System.nanoTime();
        Set<Data> keysData;
        keysData = new HashSet<Data>();
        for (K key : keys) {
            keysData.add(toData(key));
        }
        int partitionCount = clientContext.getPartitionService().getPartitionCount();
        int completionId = nextCompletionId();
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
        int partitionCount = clientContext.getPartitionService().getPartitionCount();
        int completionId = nextCompletionId();
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

    protected void storeInNearCache(Data key, Data valueData, V value,
                                    long reservationId, boolean cacheOnUpdate) {
        if (nearCache == null) {
            return;
        }

        if (cacheOnUpdate) {
            Object valueToStore = nearCache.selectToSave(value, valueData);
            nearCache.put(key, valueToStore);
            return;
        }

        if (reservationId != NOT_RESERVED) {
            if (valueData == null) {
                // we are not caching nulls in near cache for jcache
                nearCache.remove(key);
            } else {
                Object valueToStore = nearCache.selectToSave(value, valueData);
                nearCache.tryPublishReserved(key, valueToStore, reservationId, false);
            }
        }
    }

    protected void invalidateNearCache(Data key) {
        if (nearCache == null || key == null) {
            return;
        }

        nearCache.remove(key);
    }

    protected void addListenerLocally(String regId, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        if (cacheEntryListenerConfiguration.isSynchronous()) {
            syncListenerRegistrations.putIfAbsent(cacheEntryListenerConfiguration, regId);
        } else {
            asyncListenerRegistrations.putIfAbsent(cacheEntryListenerConfiguration, regId);
        }
    }

    protected String removeListenerLocally(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        ConcurrentMap<CacheEntryListenerConfiguration, String> regs;
        if (cacheEntryListenerConfiguration.isSynchronous()) {
            regs = syncListenerRegistrations;
        } else {
            regs = asyncListenerRegistrations;
        }
        return regs.remove(cacheEntryListenerConfiguration);
    }

    protected String getListenerIdLocal(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        ConcurrentMap<CacheEntryListenerConfiguration, String> regs;
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

    @Override
    public void countDownCompletionLatch(int countDownLatchId) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
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

    protected void waitCompletionLatch(Integer countDownLatchId, ICompletableFuture future) throws ExecutionException {
        if (countDownLatchId != IGNORE_COMPLETION) {
            CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
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

    private final class CacheEventHandler extends CacheAddEntryListenerCodec.AbstractEventHandler
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

    /**
     * Needed to deal with client compatibility issues.
     * Eventual consistency for near cache can be used with server versions >= 3.8.
     * Other connected server versions must use {@link Pre38NearCacheEventHandler}
     */
    private final class ConnectedServerVersionAwareNearCacheEventHandler implements EventHandler<ClientMessage> {

        private final RepairableNearCacheEventHandler repairingEventHandler = new RepairableNearCacheEventHandler();
        private final Pre38NearCacheEventHandler pre38EventHandler = new Pre38NearCacheEventHandler();
        private volatile boolean supportsRepairableNearCache;

        @Override
        public void beforeListenerRegister() {
            pre38EventHandler.beforeListenerRegister();
            repairingEventHandler.beforeListenerRegister();
        }

        @Override
        public void onListenerRegister() {
            supportsRepairableNearCache = supportsRepairableNearCache();

            if (supportsRepairableNearCache) {
                repairingEventHandler.onListenerRegister();
            } else {
                pre38EventHandler.onListenerRegister();
            }
        }

        @Override
        public void handle(ClientMessage clientMessage) {
            if (supportsRepairableNearCache) {
                repairingEventHandler.handle(clientMessage);
            } else {
                pre38EventHandler.handle(clientMessage);
            }
        }
    }

    /**
     * This event handler can only be used with server versions >= 3.8 and supports near cache eventual consistency improvements.
     * For repairing functionality please see {@link RepairingHandler}
     */
    private final class RepairableNearCacheEventHandler extends CacheAddNearCacheInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private volatile RepairingHandler repairingHandler;

        @Override
        public void beforeListenerRegister() {
            nearCache.clear();
            getRepairingTask().deregisterHandler(nameWithPrefix);
        }

        @Override
        public void onListenerRegister() {
            nearCache.clear();
            repairingHandler = getRepairingTask().registerAndGetHandler(nameWithPrefix, nearCache);
        }

        @Override
        public void handle(String name, Data key, String sourceUuid, UUID partitionUuid, long sequence) {
            repairingHandler.handle(key, sourceUuid, partitionUuid, sequence);
        }

        @Override
        public void handle(String name, Collection<Data> keys, Collection<String> sourceUuids,
                           Collection<UUID> partitionUuids, Collection<Long> sequences) {
            repairingHandler.handle(keys, sourceUuids, partitionUuids, sequences);
        }

        private RepairingTask getRepairingTask() {
            ClientContext clientContext = getContext();
            return clientContext.getRepairingTask(CacheService.SERVICE_NAME);
        }
    }

    /**
     * This event handler is here to be used with server versions < 3.8.
     * <p>
     * If server version is < 3.8 and client version is >= 3.8, this event handler must be used to
     * listen near cache invalidations. Because new improvements for near cache eventual consistency cannot work
     * with server versions < 3.8.
     */
    private final class Pre38NearCacheEventHandler extends CacheAddInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private String clientUuid;

        private Pre38NearCacheEventHandler() {
            this.clientUuid = getContext().getClusterService().getLocalClient().getUuid();
        }

        @Override
        public void handle(String name, Data key, String sourceUuid, UUID partitionUuid, long sequence) {
            if (clientUuid.equals(sourceUuid)) {
                return;
            }
            if (key != null) {
                nearCache.remove(key);
            } else {
                nearCache.clear();
            }
        }

        @Override
        public void handle(String name, Collection<Data> keys, Collection<String> sourceUuids,
                           Collection<UUID> partitionUuids, Collection<Long> sequences) {
            if (sourceUuids != null && !sourceUuids.isEmpty()) {
                Iterator<Data> keysIt = keys.iterator();
                Iterator<String> sourceUuidsIt = sourceUuids.iterator();
                while (keysIt.hasNext() && sourceUuidsIt.hasNext()) {
                    Data key = keysIt.next();
                    String sourceUuid = sourceUuidsIt.next();
                    if (!clientUuid.equals(sourceUuid)) {
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
        if (nearCache == null || !nearCache.isInvalidatedOnChange()) {
            return;
        }

        ListenerMessageCodec listenerCodec = createInvalidationListenerCodec();
        ClientListenerService listenerService = clientContext.getListenerService();

        EventHandler eventHandler = createInvalidationEventHandler();
        nearCacheMembershipRegistrationId = listenerService.registerListener(listenerCodec, eventHandler);
    }

    private EventHandler createInvalidationEventHandler() {
        return new ConnectedServerVersionAwareNearCacheEventHandler();
    }

    private ListenerMessageCodec createInvalidationListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                if (supportsRepairableNearCache()) {
                    // this is for servers >= 3.8
                    return CacheAddNearCacheInvalidationListenerCodec.encodeRequest(nameWithPrefix, localOnly);
                }
                // this is for servers < 3.8
                return CacheAddInvalidationListenerCodec.encodeRequest(nameWithPrefix, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                if (supportsRepairableNearCache()) {
                    // this is for servers >= 3.8
                    return CacheAddNearCacheInvalidationListenerCodec.decodeResponse(clientMessage).response;
                }
                // this is for servers < 3.8
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

    private int getConnectedServerVersion() {
        ClientContext clientContext = getContext();
        ClientClusterService clusterService = clientContext.getClusterService();
        Address ownerConnectionAddress = clusterService.getOwnerConnectionAddress();

        HazelcastClientInstanceImpl client = getClient();
        ClientConnectionManager connectionManager = client.getConnectionManager();
        Connection connection = connectionManager.getConnection(ownerConnectionAddress);
        if (connection == null) {
            logger.warning(format("No owner connection is available, "
                    + "near cached cache %s will be started in legacy mode", name));
            return UNKNOWN_HAZELCAST_VERSION;
        }

        return ((ClientConnection) connection).getConnectedServerVersion();
    }

    private boolean supportsRepairableNearCache() {
        return getConnectedServerVersion() >= minConsistentNearCacheSupportingServerVersion;
    }

    private void removeInvalidationListener() {
        if (nearCache != null && nearCache.isInvalidatedOnChange()) {
            String registrationId = nearCacheMembershipRegistrationId;
            if (registrationId != null) {
                clientContext.getRepairingTask(SERVICE_NAME).deregisterHandler(name);
                clientContext.getListenerService().deregisterListener(registrationId);
            }
        }
    }
}
