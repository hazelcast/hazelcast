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

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventListenerAdaptor;
import com.hazelcast.cache.impl.CacheSyncListenerCompleter;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheClearCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAndRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAndReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllKeysCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CacheReplaceCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.Data;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateConfiguredTypes;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.ExceptionUtil.rethrowAllowedTypeFirst;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;

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
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:parameternumber"})
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
    protected int partitionCount;

    private final ConcurrentMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations;
    private final ConcurrentMap<Integer, CountDownLatch> syncLocks;

    AbstractClientInternalCacheProxy(CacheConfig<K, V> cacheConfig, ClientContext context) {
        super(cacheConfig, context);
        this.asyncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        this.syncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        this.syncLocks = new ConcurrentHashMap<Integer, CountDownLatch>();
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        ClientPartitionService partitionService = getContext().getPartitionService();
        partitionCount = partitionService.getPartitionCount();
    }

    @Override
    public void setCacheManager(HazelcastCacheManager cacheManager) {
        assert cacheManager instanceof HazelcastClientCacheManager;

        this.cacheManager = (HazelcastClientCacheManager) cacheManager;
    }

    @Override
    protected void postDestroy() {
        if (cacheManager != null) {
            cacheManager.destroyCache(getName());
        }
    }

    @Override
    public void close() {
        if (statisticsEnabled) {
            statsHandler.clear();
        }

        super.close();
    }

    @Override
    protected void onDestroy() {
        if (statisticsEnabled) {
            statsHandler.clear();
        }

        super.onDestroy();
    }

    protected ClientInvocationFuture invoke(ClientMessage req, int partitionId, int completionId) {
        boolean completionOperation = completionId != -1;
        if (completionOperation) {
            registerCompletionLatch(completionId, 1);
        }
        try {
            ClientInvocation clientInvocation = new ClientInvocation(getClient(), req, partitionId);
            ClientInvocationFuture future = clientInvocation.invoke();
            if (completionOperation) {
                waitCompletionLatch(completionId, future);
            }
            return future;
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
        int partitionId = getContext().getPartitionService().getPartitionId(keyData);
        return invoke(req, partitionId, completionId);
    }

    protected <T> T getSafely(Future<T> future) {
        try {
            return future.get();
        } catch (Throwable throwable) {
            throw rethrow(throwable);
        }
    }

    protected <T> ICompletableFuture<T> getAndRemoveAsyncInternal(K key) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        validateNotNull(key);
        validateConfiguredTypes(cacheConfig, key);

        Data keyData = toData(key);
        return getAndRemoveAsyncInternal0(key, keyData, startNanos);
    }

    protected <T> ICompletableFuture<T> getAndRemoveAsyncInternal0(K key, Data keyData, long startNanos) {
        ClientDelegatingFuture<T> delegatingFuture = getAndRemoveInternal(keyData, false);
        ExecutionCallback<T> callback = !statisticsEnabled ? null : statsHandler.<T>newOnRemoveCallback(true, startNanos);
        addCallback(delegatingFuture, callback);
        return delegatingFuture;
    }

    protected Object getAndRemoveSyncInternal(K key) {
        ensureOpen();
        validateNotNull(key);
        validateConfiguredTypes(cacheConfig, key);

        Data keyData = toData(key);
        return getAndRemoveSyncInternal0(key, keyData);
    }

    protected Object getAndRemoveSyncInternal0(K key, Data keyData) {
        try {
            ClientDelegatingFuture future = getAndRemoveInternal(keyData, true);
            return future.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private <T> ClientDelegatingFuture<T> getAndRemoveInternal(Data keyData, boolean withCompletionEvent) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheGetAndRemoveCodec.encodeRequest(nameWithPrefix, keyData, completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        return newDelegatingFuture(future, GET_AND_REMOVE_RESPONSE_DECODER);
    }

    protected Object removeAsyncInternal(K key, V oldValue, boolean hasOldValue, boolean withCompletionEvent, boolean async) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue);
            validateConfiguredTypes(cacheConfig, key, oldValue);
        } else {
            validateNotNull(key);
            validateConfiguredTypes(cacheConfig, key);
        }

        Data keyData = toData(key);
        Data oldValueData = toData(oldValue);

        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheRemoveCodec.encodeRequest(nameWithPrefix, keyData, oldValueData, completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        ClientDelegatingFuture delegatingFuture = newDelegatingFuture(future, REMOVE_RESPONSE_DECODER);
        if (async) {
            ExecutionCallback callback = !statisticsEnabled ? null : statsHandler.newOnRemoveCallback(false, startNanos);
            onRemoveAsyncInternal(key, keyData, delegatingFuture, callback);
            return delegatingFuture;
        } else {
            try {
                Object result = delegatingFuture.get();
                onRemoveSyncInternal(key, keyData);
                return result;
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }
    }

    public void onRemoveSyncInternal(K key, Data keyData) {
        // NOP
    }

    protected void onRemoveAsyncInternal(K key, Data keyData, ClientDelegatingFuture future, ExecutionCallback callback) {
        addCallback(future, callback);
    }

    protected boolean replaceSyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy, boolean hasOldValue) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            validateConfiguredTypes(cacheConfig, key, newValue);
        }

        Data keyData = toData(key);
        Data oldValueData = toData(oldValue);
        Data newValueData = toData(newValue);
        Data expiryPolicyData = toData(expiryPolicy);

        return replaceSyncInternal0(key, newValue, keyData, oldValueData, newValueData, expiryPolicyData, startNanos);
    }

    protected boolean replaceSyncInternal0(K key, V newValue, Data keyData, Data oldValueData,
                                           Data newValueData, Data expiryPolicyData, long startNanos) {
        try {
            Future<Boolean> future = replaceAsyncInternal0(key, newValue, keyData, oldValueData,
                    newValueData, expiryPolicyData, null, true, startNanos);
            boolean replaced = future.get();
            if (statisticsEnabled) {
                statsHandler.onReplace(false, startNanos, replaced);
            }
            return replaced;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected <T> ICompletableFuture<T> replaceAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                             boolean hasOldValue, boolean withCompletionEvent) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            validateConfiguredTypes(cacheConfig, key, newValue);
        }

        Data keyData = toData(key);
        Data oldValueData = toData(oldValue);
        Data newValueData = toData(newValue);
        Data expiryPolicyData = toData(expiryPolicy);
        ExecutionCallback<T> callback = statisticsEnabled ? statsHandler.<T>newOnReplaceCallback(startNanos) : null;

        return replaceAsyncInternal0(key, newValue, keyData, oldValueData,
                newValueData, expiryPolicyData, callback, withCompletionEvent, startNanos);
    }

    protected <T> ICompletableFuture<T> replaceAsyncInternal0(K key, V newValue, Data keyData, Data oldValueData,
                                                              Data newValueData, Data expiryPolicyData,
                                                              ExecutionCallback<T> callback, boolean withCompletionEvent,
                                                              long startNanos) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheReplaceCodec.encodeRequest(nameWithPrefix, keyData, oldValueData, newValueData,
                expiryPolicyData, completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        ClientDelegatingFuture<T> delegatingFuture = newDelegatingFuture(future, REPLACE_RESPONSE_DECODER);
        addCallback(delegatingFuture, callback);
        return delegatingFuture;
    }

    protected Object replaceAndGetSyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                               boolean hasOldValue, boolean withCompletionEvent) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            validateConfiguredTypes(cacheConfig, key, newValue);
        }

        Data keyData = toData(key);
        Data newValueData = toData(newValue);
        Data expiryPolicyData = toData(expiryPolicy);

        return replaceAndGetSyncInternal0(key, newValue, keyData, newValueData,
                expiryPolicyData, withCompletionEvent, startNanos);
    }

    protected Object replaceAndGetSyncInternal0(K key, V newValue, Data keyData, Data newValueData, Data expiryPolicyData,
                                                boolean withCompletionEvent, long startNanos) {
        try {
            ICompletableFuture<Object> future = replaceAndGetAsyncInternal0(keyData, newValueData, expiryPolicyData,
                    withCompletionEvent, startNanos);
            Object response = future.get();

            if (statisticsEnabled) {
                statsHandler.onReplace(true, startNanos, response);
            }
            return response;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected <T> ICompletableFuture<T> replaceAndGetAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                                   boolean hasOldValue, boolean withCompletionEvent) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            validateConfiguredTypes(cacheConfig, key, newValue);
        }

        Data keyData = toData(key);
        Data newValueData = toData(newValue);
        Data expiryPolicyData = toData(expiryPolicy);
        return replaceAndGetAsyncInternal0(keyData, newValueData, expiryPolicyData, withCompletionEvent, startNanos);
    }

    protected <T> ICompletableFuture<T> replaceAndGetAsyncInternal0(Data keyData, Data newValueData, Data expiryPolicyData,
                                                                    boolean withCompletionEvent, long startNanos) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheGetAndReplaceCodec.encodeRequest(nameWithPrefix, keyData, newValueData, expiryPolicyData,
                completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        ClientDelegatingFuture<T> delegatingFuture = newDelegatingFuture(future, GET_AND_REPLACE_RESPONSE_DECODER);
        ExecutionCallback<T> callback = statisticsEnabled ? statsHandler.<T>newOnReplaceCallback(startNanos) : null;
        addCallback(delegatingFuture, callback);
        return delegatingFuture;
    }

    protected static <T> void addCallback(ClientDelegatingFuture<T> delegatingFuture, ExecutionCallback<T> callback) {
        if (callback == null) {
            return;
        }
        delegatingFuture.andThenInternal(callback, true);
    }

    private ClientInvocationFuture putInternal(Data keyData, Data valueData,
                                               Data expiryPolicyData, boolean isGet, boolean withCompletionEvent) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CachePutCodec.encodeRequest(nameWithPrefix, keyData, valueData, expiryPolicyData, isGet,
                completionId);
        return invoke(request, keyData, completionId);
    }

    protected V putSyncInternal(K key, V value, ExpiryPolicy expiryPolicy, boolean isGet) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        validateNotNull(key, value);
        validateConfiguredTypes(cacheConfig, key, value);

        Data keyData = toData(key);
        Data valueData = toData(value);

        return putSyncInternal0(key, value, keyData, valueData, expiryPolicy, isGet, startNanos);

    }

    protected V putSyncInternal0(K key, V value, Data keyData, Data valueData,
                                 ExpiryPolicy expiryPolicy, boolean isGet, long startNanos) {
        Data expiryPolicyData = toData(expiryPolicy);

        try {
            ClientInvocationFuture invocationFuture = putInternal(keyData, valueData, expiryPolicyData, isGet, true);
            ClientDelegatingFuture<V> delegatingFuture = newDelegatingFuture(invocationFuture, PUT_RESPONSE_DECODER);
            V response = delegatingFuture.get();

            if (statisticsEnabled) {
                statsHandler.onPut(isGet, startNanos, response != null);
            }
            return response;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected ClientDelegatingFuture putAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy, boolean isGet,
                                                      boolean withCompletionEvent, OneShotExecutionCallback<V> callback) {
        ensureOpen();
        validateNotNull(key, value);
        validateConfiguredTypes(cacheConfig, key, value);

        Data keyData = toData(key);
        Data valueData = toData(value);
        Data expiryPolicyData = toData(expiryPolicy);

        return putAsyncInternal0(key, value, keyData, valueData, expiryPolicyData, isGet, withCompletionEvent, callback);
    }

    protected ClientDelegatingFuture putAsyncInternal0(K key, V value, Data keyData, Data valueData, Data expiryPolicyData,
                                                       boolean isGet, boolean withCompletionEvent,
                                                       OneShotExecutionCallback<V> callback) {
        ClientInvocationFuture invocationFuture = putInternal(keyData, valueData, expiryPolicyData, isGet, withCompletionEvent);
        if (callback == null) {
            return newDelegatingFuture(invocationFuture, PUT_RESPONSE_DECODER);
        }

        CallbackAwareClientDelegatingFuture<V> future = new CallbackAwareClientDelegatingFuture<V>(invocationFuture,
                getSerializationService(), PUT_RESPONSE_DECODER, callback);
        future.andThenInternal(callback, true);
        return future;
    }

    protected OneShotExecutionCallback<V> newStatsCallbackOrNull(boolean isGet) {
        if (!statisticsEnabled) {
            return null;
        }
        return statsHandler.newOnPutCallback(isGet, System.nanoTime());
    }

    protected Object putIfAbsentSyncInternal(K key, V value, ExpiryPolicy expiryPolicy,
                                             boolean withCompletionEvent, boolean async) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        validateNotNull(key, value);
        validateConfiguredTypes(cacheConfig, key, value);

        Data keyData = toData(key);
        Data valueData = toData(value);
        Data expiryPolicyData = toData(expiryPolicy);

        if (async) {
            ExecutionCallback<Boolean> callback = !statisticsEnabled ? null : statsHandler.newOnPutIfAbsentCallback(startNanos);
            return putIfAbsentAsyncInternal0(key, value, keyData, valueData,
                    expiryPolicyData, callback, withCompletionEvent, startNanos);
        } else {
            return putIfAbsentSyncInternal0(key, value, keyData, valueData,
                    expiryPolicyData, withCompletionEvent, startNanos);
        }
    }

    protected Object putIfAbsentSyncInternal0(K key, V value, Data keyData, Data valueData, Data expiryPolicyData,
                                              boolean withCompletionEvent, long startNanos) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CachePutIfAbsentCodec.encodeRequest(nameWithPrefix, keyData, valueData,
                expiryPolicyData, completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        ClientDelegatingFuture<Boolean> delegatingFuture = newDelegatingFuture(future, PUT_IF_ABSENT_RESPONSE_DECODER);
        try {
            Object response = delegatingFuture.get();

            if (statisticsEnabled) {
                statsHandler.onPutIfAbsent(startNanos, (Boolean) response);
            }
            return response;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected ClientDelegatingFuture<Boolean> putIfAbsentAsyncInternal0(K key, V value, Data keyData, Data valueData,
                                                                        Data expiryPolicyData,
                                                                        ExecutionCallback<Boolean> callback,
                                                                        boolean withCompletionEvent, long startNanos) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CachePutIfAbsentCodec.encodeRequest(nameWithPrefix, keyData, valueData,
                expiryPolicyData, completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        ClientDelegatingFuture<Boolean> delegatingFuture = newDelegatingFuture(future, PUT_IF_ABSENT_RESPONSE_DECODER);
        addCallback(delegatingFuture, callback);
        return delegatingFuture;
    }

    protected void removeAllKeysInternal(Set<? extends K> keys, Collection<Data> dataKeys, long startNanos) {
        int partitionCount = getContext().getPartitionService().getPartitionCount();
        int completionId = nextCompletionId();
        registerCompletionLatch(completionId, partitionCount);
        ClientMessage request = CacheRemoveAllKeysCodec.encodeRequest(nameWithPrefix, dataKeys, completionId);
        try {
            invoke(request);
            waitCompletionLatch(completionId, null);
            if (statisticsEnabled) {
                statsHandler.onBatchRemove(startNanos, dataKeys.size());
            }
        } catch (Throwable t) {
            deregisterCompletionLatch(completionId);
            throw rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    protected void removeAllInternal() {
        int partitionCount = getContext().getPartitionService().getPartitionCount();
        int completionId = nextCompletionId();
        registerCompletionLatch(completionId, partitionCount);
        ClientMessage request = CacheRemoveAllCodec.encodeRequest(nameWithPrefix, completionId);
        try {
            invoke(request);
            waitCompletionLatch(completionId, null);
            if (statisticsEnabled) {
                statsHandler.getStatistics().setLastUpdateTime(System.currentTimeMillis());
                // we don't support count stats of removing all entries
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
                statsHandler.getStatistics().setLastUpdateTime(System.currentTimeMillis());
                // we don't support count stats of removing all entries
            }
        } catch (Throwable t) {
            throw rethrowAllowedTypeFirst(t, CacheException.class);
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
        ClientListenerService listenerService = getContext().getListenerService();
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

    private void awaitLatch(CountDownLatch countDownLatch, ICompletableFuture future) throws ExecutionException {
        try {
            long currentTimeoutMs = MAX_COMPLETION_LATCH_WAIT_TIME;
            // Call latch await in small steps to be able to check if node is still active.
            // If not active then throw HazelcastInstanceNotActiveException,
            // If closed or destroyed then throw IllegalStateException,
            // otherwise continue to wait until `MAX_COMPLETION_LATCH_WAIT_TIME` passes.
            //
            // Warning: Silently ignoring if latch does not countdown in time.
            while (currentTimeoutMs > 0
                    && !countDownLatch.await(COMPLETION_LATCH_WAIT_TIME_STEP, TimeUnit.MILLISECONDS)) {
                if (future != null && future.isDone()) {
                    Object response = future.get();
                    if (response instanceof Throwable) {
                        return;
                    }
                }
                currentTimeoutMs -= COMPLETION_LATCH_WAIT_TIME_STEP;
                if (!getContext().isActive()) {
                    throw new HazelcastInstanceNotActiveException();
                } else if (isClosed()) {
                    throw new IllegalStateException("Cache (" + nameWithPrefix + ") is closed!");
                } else if (isDestroyed()) {
                    throw new IllegalStateException("Cache (" + nameWithPrefix + ") is destroyed!");
                }
            }
            if (countDownLatch.getCount() > 0) {
                logger.finest("Countdown latch wait timeout after " + MAX_COMPLETION_LATCH_WAIT_TIME + " milliseconds!");
            }
        } catch (InterruptedException e) {
            sneakyThrow(e);
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
}
