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

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.impl.CacheEventListenerAdaptor;
import com.hazelcast.cache.impl.CacheSyncListenerCompleter;
import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheClearCodec;
import com.hazelcast.client.impl.protocol.codec.CacheContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.CacheEntryProcessorCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAndRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAndReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetCodec;
import com.hazelcast.client.impl.protocol.codec.CacheListenerRegistrationCodec;
import com.hazelcast.client.impl.protocol.codec.CacheLoadAllCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutAllCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllKeysCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CacheReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.CacheSetExpiryPolicyCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientListenerService;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.internal.util.FutureUtil;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessorException;
import java.io.Closeable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.cache.impl.CacheProxyUtil.NULL_KEY_IS_NOT_ALLOWED;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateConfiguredTypes;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.client.cache.impl.ClientCacheProxySupportUtil.addCallback;
import static com.hazelcast.client.cache.impl.ClientCacheProxySupportUtil.getSafely;
import static com.hazelcast.client.cache.impl.ClientCacheProxySupportUtil.handleFailureOnCompletionListener;
import static com.hazelcast.internal.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.ExceptionUtil.rethrowAllowedTypeFirst;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;

/**
 * Abstract {@link com.hazelcast.cache.ICache} implementation which provides shared internal implementations
 * of cache operations like put, replace, remove, invoke and open/close operations. These internal implementations are delegated
 * by actual cache methods.
 * <p>
 * Note: this partial implementation is used by client.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
abstract class ClientCacheProxySupport<K, V> extends ClientProxy implements ICacheInternal<K, V>,
                                                                            CacheSyncListenerCompleter {

    private static final int TIMEOUT = 10;

    private static final CompletionListener NULL_COMPLETION_LISTENER = new ClientCacheProxySupportUtil.EmptyCompletionListener();

    // this will represent the name from the user perspective
    protected final String name;
    protected final String nameWithPrefix;
    protected final CacheConfig<K, V> cacheConfig;


    protected int partitionCount;

    boolean statisticsEnabled;
    CacheStatsHandler statsHandler;

    private ILogger logger;
    private final AtomicReference<HazelcastClientCacheManager> cacheManagerRef = new AtomicReference<>();
    private final ConcurrentMap<Future, CompletionListener> loadAllCalls = new ConcurrentHashMap<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

    private final AtomicInteger completionIdCounter = new AtomicInteger();

    private final ClientCacheProxySyncListenerCompleter listenerCompleter;
    private final ConcurrentMap<UUID, Closeable> closeableListeners;

    protected ClientCacheProxySupport(CacheConfig<K, V> cacheConfig, ClientContext context) {
        super(ICacheService.SERVICE_NAME, cacheConfig.getName(), context);
        this.name = cacheConfig.getName();
        this.nameWithPrefix = cacheConfig.getNameWithPrefix();
        this.cacheConfig = cacheConfig;
        this.statisticsEnabled = cacheConfig.isStatisticsEnabled();
        this.closeableListeners = new ConcurrentHashMap<>();
        this.listenerCompleter = new ClientCacheProxySyncListenerCompleter(this);
    }

    @Override
    protected void onInitialize() {
        logger = getContext().getLoggingService().getLogger(getClass());
        statsHandler = new CacheStatsHandler(getSerializationService());
        ClientPartitionService partitionService = getContext().getPartitionService();
        partitionCount = partitionService.getPartitionCount();
    }

    @Override
    protected String getDistributedObjectName() {
        return cacheConfig.getNameWithPrefix();
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        if (statisticsEnabled) {
            statsHandler.clear();
        }
        close0(false);
    }

    @Override
    protected boolean preDestroy() {
        if (!isDestroyed.compareAndSet(false, true)) {
            return false;
        }
        close0(true);
        isClosed.set(true);
        return true;
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public boolean isDestroyed() {
        return isDestroyed.get();
    }

    @Override
    public void open() {
        if (isDestroyed.get()) {
            throw new IllegalStateException("Cache is already destroyed! Cannot be reopened");
        }
        isClosed.set(false);
    }

    @Override
    public String getPrefixedName() {
        return nameWithPrefix;
    }

    @Override
    protected <T> T invoke(ClientMessage clientMessage) {
        try {
            Future<ClientMessage> future = new ClientInvocation(getClient(), clientMessage, getName()).invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public void countDownCompletionLatch(int countDownLatchId) {
        listenerCompleter.countDownCompletionLatch(countDownLatchId);
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManagerRef.get();
    }

    @Override
    public void setCacheManager(HazelcastCacheManager cacheManager) {
        assert cacheManager instanceof HazelcastClientCacheManager;

        if (cacheManagerRef.get() == cacheManager) {
            return;
        }
        if (!cacheManagerRef.compareAndSet(null, (HazelcastClientCacheManager) cacheManager)) {
            throw new IllegalStateException("Cannot overwrite a Cache's CacheManager.");
        }
    }

    @Override
    public void resetCacheManager() {
        cacheManagerRef.set(null);
    }

    @Override
    protected void postDestroy() {
        CacheManager cacheManager = cacheManagerRef.get();
        if (cacheManager != null) {
            cacheManager.destroyCache(getName());
        }
        resetCacheManager();
    }

    @Override
    protected void onDestroy() {
        if (statisticsEnabled) {
            statsHandler.clear();
        }

        super.onDestroy();
    }

    protected void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    protected void injectDependencies(Object obj) {
        ManagedContext managedContext = getSerializationService().getManagedContext();
        managedContext.initialize(obj);
    }

    protected void onLoadAll(List<Data> keys, Object response, long startNanos) {
    }

    protected long nowInNanosOrDefault() {
        return statisticsEnabled ? System.nanoTime() : -1;
    }

    protected ClientInvocationFuture invoke(ClientMessage req, int partitionId, int completionId) {
        boolean completionOperation = completionId != -1;
        if (completionOperation) {
            listenerCompleter.registerCompletionLatch(completionId, 1);
        }
        try {
            ClientInvocation clientInvocation = new ClientInvocation(getClient(), req, name, partitionId);
            ClientInvocationFuture future = clientInvocation.invoke();
            if (completionOperation) {
                listenerCompleter.waitCompletionLatch(completionId, future);
            }
            return future;
        } catch (Throwable e) {
            if (e instanceof IllegalStateException) {
                close();
            }
            if (completionOperation) {
                listenerCompleter.deregisterCompletionLatch(completionId);
            }
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected ClientInvocationFuture invoke(ClientMessage req, Data keyData, int completionId) {
        int partitionId = getContext().getPartitionService().getPartitionId(keyData);
        return invoke(req, partitionId, completionId);
    }

    protected <T> InternalCompletableFuture<T> getAndRemoveAsyncInternal(K key) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        validateNotNull(key);
        validateConfiguredTypes(cacheConfig, key);

        Data keyData = toData(key);
        ClientDelegatingFuture<T> delegatingFuture = getAndRemoveInternal(keyData, false);
        BiConsumer<T, Throwable> callback = !statisticsEnabled
                ? null
                : statsHandler.newOnRemoveCallback(true, startNanos);
        onGetAndRemoveAsyncInternal(key, keyData, delegatingFuture, callback);
        return delegatingFuture;
    }

    protected <T> ClientDelegatingFuture<T> getAndRemoveSyncInternal(K key) {
        ensureOpen();
        validateNotNull(key);
        validateConfiguredTypes(cacheConfig, key);

        Data keyData = toData(key);
        ClientDelegatingFuture<T> delegatingFuture = getAndRemoveInternal(keyData, true);
        onGetAndRemoveAsyncInternal(key, keyData, delegatingFuture, null);
        return delegatingFuture;
    }

    protected <T> void onGetAndRemoveAsyncInternal(K key, Data keyData, ClientDelegatingFuture<T> delegatingFuture,
                                                   BiConsumer<T, Throwable> callback) {
        addCallback(delegatingFuture, callback);
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
        ClientDelegatingFuture delegatingFuture =
                newDelegatingFuture(future, clientMessage -> CacheRemoveCodec.decodeResponse(clientMessage).response);
        if (async) {
            BiConsumer<Object, Throwable> callback = !statisticsEnabled
                    ? null
                    : statsHandler.newOnRemoveCallback(false, startNanos);
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

    protected void onRemoveSyncInternal(Object key, Data keyData) {
        // NOP
    }

    protected void onRemoveAsyncInternal(Object key, Data keyData, ClientDelegatingFuture future,
                                         BiConsumer<Object, Throwable> callback) {
        addCallback(future, callback);
    }

    protected boolean replaceSyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy, boolean hasOldValue) {
        long startNanos = nowInNanosOrDefault();
        Future<Boolean> future = replaceAsyncInternal(key, oldValue, newValue, expiryPolicy, hasOldValue, true, false);
        try {
            boolean replaced = future.get();
            if (statisticsEnabled) {
                statsHandler.onReplace(false, startNanos, replaced);
            }
            return replaced;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected <T> InternalCompletableFuture<T> replaceAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                             boolean hasOldValue, boolean withCompletionEvent, boolean async) {
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
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheReplaceCodec.encodeRequest(nameWithPrefix, keyData, oldValueData, newValueData,
                expiryPolicyData, completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        ClientDelegatingFuture<T> delegatingFuture = newDelegatingFuture(future,
                message -> CacheReplaceCodec.decodeResponse(message).response);
        BiConsumer<T, Throwable> callback = async && statisticsEnabled
                ? statsHandler.newOnReplaceCallback(startNanos)
                : null;
        onReplaceInternalAsync(key, newValue, keyData, newValueData, delegatingFuture, callback);
        return delegatingFuture;
    }

    protected <T> void onReplaceInternalAsync(K key, V value, Data keyData, Data valueData,
                                              ClientDelegatingFuture<T> delegatingFuture, BiConsumer<T, Throwable> callback) {
        addCallback(delegatingFuture, callback);
    }

    protected <T> InternalCompletableFuture<T> replaceAndGetAsyncInternal(K key, V oldValue,
                                                                          V newValue, ExpiryPolicy expiryPolicy,
                                                                          boolean hasOldValue, boolean withCompletionEvent,
                                                                          boolean async) {
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

        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheGetAndReplaceCodec.encodeRequest(nameWithPrefix, keyData, newValueData, expiryPolicyData,
                completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        ClientDelegatingFuture<T> delegatingFuture =
                newDelegatingFuture(future, message -> CacheGetAndReplaceCodec.decodeResponse(message).response);
        BiConsumer<T, Throwable> callback = async && statisticsEnabled
                ? statsHandler.<T>newOnReplaceCallback(startNanos)
                : null;
        onReplaceAndGetAsync(key, newValue, keyData, newValueData, delegatingFuture, callback);
        return delegatingFuture;
    }

    protected <T> void onReplaceAndGetAsync(K key, V value, Data keyData, Data valueData,
                                            ClientDelegatingFuture<T> delegatingFuture, BiConsumer<T, Throwable> callback) {
        addCallback(delegatingFuture, callback);
    }

    protected V putSyncInternal(K key, V value, ExpiryPolicy expiryPolicy, boolean isGet) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        validateNotNull(key, value);
        validateConfiguredTypes(cacheConfig, key, value);

        Data keyData = toData(key);
        Data valueData = toData(value);
        Data expiryPolicyData = toData(expiryPolicy);

        try {
            ClientInvocationFuture invocationFuture = putInternal(keyData, valueData, expiryPolicyData, isGet, true);
            ClientDelegatingFuture<V> delegatingFuture =
                    newDelegatingFuture(invocationFuture, clientMessage -> CachePutCodec.decodeResponse(clientMessage).response);
            V response = delegatingFuture.get();

            if (statisticsEnabled) {
                statsHandler.onPut(isGet, startNanos, response != null);
            }
            return response;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        } finally {
            onPutSyncInternal(key, value, keyData, valueData);
        }
    }

    protected void onPutSyncInternal(K key, V value, Data keyData, Data valueData) {
        // NOP
    }

    protected ClientDelegatingFuture putAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy, boolean isGet,
                                                      boolean withCompletionEvent, BiConsumer<V, Throwable> callback) {
        ensureOpen();
        validateNotNull(key, value);
        validateConfiguredTypes(cacheConfig, key, value);

        Data keyData = toData(key);
        Data valueData = toData(value);
        Data expiryPolicyData = toData(expiryPolicy);

        ClientInvocationFuture invocationFuture = putInternal(keyData, valueData, expiryPolicyData, isGet, withCompletionEvent);
        return wrapPutAsyncFuture(key, value, keyData, valueData, invocationFuture, callback);
    }

    protected ClientDelegatingFuture<V> wrapPutAsyncFuture(K key, V value, Data keyData, Data valueData,
                                                           ClientInvocationFuture invocationFuture,
                                                           BiConsumer<V, Throwable> callback) {

        ClientDelegatingFuture<V> future = newDelegatingFuture(invocationFuture,
                message -> CachePutCodec.decodeResponse(message).response);
        if (callback != null) {
            future.whenComplete(callback);
        }
        return future;
    }

    protected BiConsumer<V, Throwable> newStatsCallbackOrNull(boolean isGet) {
        if (!statisticsEnabled) {
            return null;
        }
        return statsHandler.newOnPutCallback(isGet, System.nanoTime());
    }

    protected boolean setExpiryPolicyInternal(K key, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        validateNotNull(key);
        validateNotNull(expiryPolicy);

        Data keyData = toData(key);
        Data expiryPolicyData = toData(expiryPolicy);

        List<Data> list = Collections.singletonList(keyData);
        ClientMessage request = CacheSetExpiryPolicyCodec.encodeRequest(nameWithPrefix, list, expiryPolicyData);
        ClientInvocationFuture future = invoke(request, keyData, IGNORE_COMPLETION);
        ClientDelegatingFuture<Boolean> delegatingFuture =
                newDelegatingFuture(future, clientMessage -> CacheSetExpiryPolicyCodec.decodeResponse(clientMessage).response);
        try {
            return delegatingFuture.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected Object putIfAbsentInternal(K key, V value, ExpiryPolicy expiryPolicy, boolean withCompletionEvent, boolean async) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        validateNotNull(key, value);
        validateConfiguredTypes(cacheConfig, key, value);

        Data keyData = toData(key);
        Data valueData = toData(value);
        Data expiryPolicyData = toData(expiryPolicy);

        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CachePutIfAbsentCodec.encodeRequest(nameWithPrefix, keyData, valueData,
                expiryPolicyData, completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        ClientDelegatingFuture<Boolean> delegatingFuture =
                newDelegatingFuture(future, message -> CachePutIfAbsentCodec.decodeResponse(message).response);
        if (async) {
            BiConsumer<Boolean, Throwable> callback = !statisticsEnabled
                    ? null
                    : statsHandler.newOnPutIfAbsentCallback(startNanos);
            onPutIfAbsentAsyncInternal(key, value, keyData, valueData, delegatingFuture, callback);
            return delegatingFuture;
        } else {
            try {
                Boolean response = delegatingFuture.get();

                if (statisticsEnabled) {
                    statsHandler.onPutIfAbsent(startNanos, response);
                }
                onPutIfAbsentSyncInternal(key, value, keyData, valueData);
                return response;
            } catch (Throwable e) {
                throw rethrowAllowedTypeFirst(e, CacheException.class);
            }
        }
    }

    protected void onPutIfAbsentAsyncInternal(K key, V value, Data keyData, Data valueData,
                                              ClientDelegatingFuture<Boolean> delegatingFuture,
                                              BiConsumer<Boolean, Throwable> callback) {
        addCallback(delegatingFuture, callback);
    }

    protected void onPutIfAbsentSyncInternal(K key, V value, Data keyData, Data valueData) {
        // NOP
    }

    protected void removeAllKeysInternal(Set<? extends K> keys, Collection<Data> dataKeys, long startNanos) {
        int partitionCount = getContext().getPartitionService().getPartitionCount();
        int completionId = nextCompletionId();
        listenerCompleter.registerCompletionLatch(completionId, partitionCount);
        ClientMessage request = CacheRemoveAllKeysCodec.encodeRequest(nameWithPrefix, dataKeys, completionId);
        try {
            invoke(request);
            listenerCompleter.waitCompletionLatch(completionId, null);
            if (statisticsEnabled) {
                statsHandler.onBatchRemove(startNanos, dataKeys.size());
            }
        } catch (Throwable t) {
            listenerCompleter.deregisterCompletionLatch(completionId);
            throw rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    protected void removeAllInternal() {
        int partitionCount = getContext().getPartitionService().getPartitionCount();
        int completionId = nextCompletionId();
        listenerCompleter.registerCompletionLatch(completionId, partitionCount);
        ClientMessage request = CacheRemoveAllCodec.encodeRequest(nameWithPrefix, completionId);
        try {
            invoke(request);
            listenerCompleter.waitCompletionLatch(completionId, null);
            if (statisticsEnabled) {
                statsHandler.getStatistics().setLastUpdateTime(System.currentTimeMillis());
                // we don't support count stats of removing all entries
            }
        } catch (Throwable t) {
            listenerCompleter.deregisterCompletionLatch(completionId);
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

    protected void addListenerLocally(UUID regId, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration,
                                      CacheEventListenerAdaptor<K, V> adaptor) {
        listenerCompleter.putListenerIfAbsent(cacheEntryListenerConfiguration, regId);
        CacheEntryListener<K, V> entryListener = adaptor.getCacheEntryListener();
        if (entryListener instanceof Closeable) {
            closeableListeners.putIfAbsent(regId, (Closeable) entryListener);
        }
    }

    protected void removeListenerLocally(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        UUID registrationId = listenerCompleter.removeListener(cacheEntryListenerConfiguration);
        if (registrationId != null) {
            Closeable closeable = closeableListeners.remove(registrationId);
            IOUtil.closeResource(closeable);
        }
    }

    protected UUID getListenerIdLocal(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        return listenerCompleter.getListenerId(cacheEntryListenerConfiguration);
    }

    private void deregisterAllCacheEntryListener(Collection<UUID> listenerRegistrations) {
        ClientListenerService listenerService = getContext().getListenerService();
        for (UUID regId : listenerRegistrations) {
            listenerService.deregisterListener(regId);
        }
    }

    protected V getSyncInternal(Object key, ExpiryPolicy expiryPolicy) {
        long startNanos = nowInNanosOrDefault();
        try {
            ClientDelegatingFuture<V> future = getInternal(key, expiryPolicy, false);
            V value = future.get();
            if (statisticsEnabled) {
                statsHandler.onGet(startNanos, value != null);
            }
            return value;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected InternalCompletableFuture<V> getAsyncInternal(Object key, ExpiryPolicy expiryPolicy,
                                                            BiConsumer<V, Throwable> callback) {
        Data dataKey = toData(key);
        ClientDelegatingFuture<V> future = getInternal(dataKey, expiryPolicy, true);
        addCallback(future, callback);
        return future;
    }

    protected void getAllInternal(Set<? extends K> keys, Collection<Data> dataKeys, ExpiryPolicy expiryPolicy,
                                  List<Object> resultingKeyValuePairs, long startNanos) {
        if (dataKeys.isEmpty()) {
            objectToDataCollection(keys, dataKeys, getSerializationService(), NULL_KEY_IS_NOT_ALLOWED);
        }
        Data expiryPolicyData = toData(expiryPolicy);

        ClientMessage request = CacheGetAllCodec.encodeRequest(nameWithPrefix, dataKeys, expiryPolicyData);
        ClientMessage responseMessage = invoke(request);

        List<Map.Entry<Data, Data>> response = CacheGetAllCodec.decodeResponse(responseMessage).response;
        for (Map.Entry<Data, Data> entry : response) {
            resultingKeyValuePairs.add(entry.getKey());
            resultingKeyValuePairs.add(entry.getValue());
        }

        if (statisticsEnabled) {
            statsHandler.onBatchGet(startNanos, response.size());
        }
    }

    protected void setExpiryPolicyInternal(Set<? extends K> keys, ExpiryPolicy policy) {
        setExpiryPolicyInternal(keys, policy, null);
    }

    protected void setExpiryPolicyInternal(Set<? extends K> keys, ExpiryPolicy policy, Set<Data> serializedKeys) {
        try {
            List<Data>[] keysByPartition = groupKeysToPartitions(keys, serializedKeys);
            setExpiryPolicyAndWaitForCompletion(keysByPartition, policy);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected void putAllInternal(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy, Map<Object, Data> keyMap,
                                  List<Map.Entry<Data, Data>>[] entriesPerPartition, long startNanos) {
        try {
            // first we fill entry set per partition
            groupDataToPartitions(map, getContext().getPartitionService(), keyMap, entriesPerPartition);
            // then we invoke the operations and sync on completion of these operations
            putToAllPartitionsAndWaitForCompletion(entriesPerPartition, expiryPolicy, startNanos);
        } catch (Exception t) {
            throw rethrow(t);
        }
    }

    protected boolean containsKeyInternal(Object key) {
        Data keyData = toData(key);
        ClientMessage request = CacheContainsKeyCodec.encodeRequest(nameWithPrefix, keyData);
        ClientMessage result = invoke(request, keyData);
        return CacheContainsKeyCodec.decodeResponse(result).response;
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

    protected Object invokeInternal(Object key, Data epData, Object... arguments) {
        Data keyData = toData(key);
        List<Data> argumentsData;
        if (arguments != null) {
            argumentsData = new ArrayList<>(arguments.length);
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
            InternalCompletableFuture<ClientMessage> future = invoke(request, keyData, completionId);
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

    protected ILogger getLogger() {
        return logger;
    }

    private ClientDelegatingFuture<V> getInternal(Object key, ExpiryPolicy expiryPolicy, boolean deserializeResponse) {
        Data keyData = toData(key);
        Data expiryPolicyData = toData(expiryPolicy);

        ClientMessage request = CacheGetCodec.encodeRequest(nameWithPrefix, keyData, expiryPolicyData);
        int partitionId = getContext().getPartitionService().getPartitionId(keyData);

        ClientInvocation clientInvocation = new ClientInvocation(getClient(), request, name, partitionId);
        ClientInvocationFuture future = clientInvocation.invoke();
        return newDelegatingFuture(future, message -> CacheGetCodec.decodeResponse(message).response, deserializeResponse);
    }

    private void groupDataToPartitions(Map<? extends K, ? extends V> map, ClientPartitionService partitionService,
                                       Map<Object, Data> keyMap, List<Map.Entry<Data, Data>>[] entriesPerPartition) {
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            K key = entry.getKey();
            V value = entry.getValue();
            validateNotNull(key, value);

            Data keyData = toData(key);
            Data valueData = toData(value);

            if (keyMap != null) {
                keyMap.put(key, keyData);
            }

            int partitionId = partitionService.getPartitionId(keyData);
            List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
            if (entries == null) {
                entries = new ArrayList<>();
                entriesPerPartition[partitionId] = entries;
            }
            entries.add(new AbstractMap.SimpleImmutableEntry<>(keyData, valueData));
        }
    }

    private List<Data>[] groupKeysToPartitions(Set<? extends K> keys, Set<Data> serializedKeys) {
        List<Data>[] keysByPartition = new List[partitionCount];
        ClientPartitionService partitionService = getContext().getPartitionService();
        for (K key : keys) {
            Data keyData = getSerializationService().toData(key);

            if (serializedKeys != null) {
                serializedKeys.add(keyData);
            }

            int partitionId = partitionService.getPartitionId(keyData);

            List<Data> partition = keysByPartition[partitionId];
            if (partition == null) {
                partition = new ArrayList<Data>();
                keysByPartition[partitionId] = partition;
            }
            partition.add(keyData);
        }
        return keysByPartition;
    }

    private void putToAllPartitionsAndWaitForCompletion(List<Map.Entry<Data, Data>>[] entriesPerPartition,
                                                        ExpiryPolicy expiryPolicy, long startNanos) {
        Data expiryPolicyData = toData(expiryPolicy);
        List<ClientCacheProxySupportUtil.FutureEntriesTuple> futureEntriesTuples = new ArrayList<>(entriesPerPartition.length);

        for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
            List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];

            if (entries != null) {
                int completionId = nextCompletionId();
                // TODO: if there is a single entry, we could make use of a put operation since that is a bit cheaper
                ClientMessage request = CachePutAllCodec.encodeRequest(nameWithPrefix, entries, expiryPolicyData, completionId);
                Future future = invoke(request, partitionId, completionId);
                futureEntriesTuples.add(new ClientCacheProxySupportUtil.FutureEntriesTuple(future, entries));
            }
        }

        waitResponseFromAllPartitionsForPutAll(futureEntriesTuples, startNanos);
    }

    private void setExpiryPolicyAndWaitForCompletion(List<Data>[] keysByPartition, ExpiryPolicy expiryPolicy) {
        List<Future> futures = new ArrayList<>(keysByPartition.length);

        Data policyData = toData(expiryPolicy);
        for (int partitionId = 0; partitionId < keysByPartition.length; partitionId++) {
            List<Data> keys = keysByPartition[partitionId];
            if (keys != null) {
                ClientMessage request = CacheSetExpiryPolicyCodec.encodeRequest(nameWithPrefix, keys, policyData);
                futures.add(invoke(request, partitionId, IGNORE_COMPLETION));
            }
        }

        List<Throwable> throwables = FutureUtil.waitUntilAllResponded(futures);
        if (throwables.size() > 0) {
            throw rethrow(throwables.get(0));
        }
    }

    private void waitResponseFromAllPartitionsForPutAll(List<ClientCacheProxySupportUtil.FutureEntriesTuple> futureEntriesTuples,
                                                        long startNanos) {
        Throwable error = null;
        for (ClientCacheProxySupportUtil.FutureEntriesTuple tuple : futureEntriesTuples) {
            Future future = tuple.getFuture();
            List<Map.Entry<Data, Data>> entries = tuple.getEntries();
            try {
                future.get();
                // Note that we count the batch put only if there is no exception while putting to target partition.
                // In case of error, some of the entries might have been put and others might fail.
                // But we simply ignore the actual put count here if there is an error.
                if (statisticsEnabled) {
                    statsHandler.getStatistics().increaseCachePuts(entries.size());
                }
            } catch (Throwable t) {
                logger.finest("Error occurred while putting entries as batch!", t);
                if (error == null) {
                    error = t;
                }
            }
        }

        if (statisticsEnabled) {
            statsHandler.getStatistics().addPutTimeNanos(nowInNanosOrDefault() - startNanos);
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
            throw rethrow(error);
        }
    }

    private int nextCompletionId() {
        return completionIdCounter.incrementAndGet();
    }

    private void close0(boolean destroy) {
        waitOnGoingLoadAllCallsToFinish();
        closeListeners();
        if (!destroy) {
            // when cache is being destroyed, the CacheManager is still required for cleanup and reset in postDestroy
            // when cache is being closed, the CacheManager is reset now
            resetCacheManager();
        }
    }

    private void waitOnGoingLoadAllCallsToFinish() {
        Iterator<Map.Entry<Future, CompletionListener>> iterator = loadAllCalls.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Future, CompletionListener> entry = iterator.next();
            Future future = entry.getKey();
            CompletionListener completionListener = entry.getValue();
            try {
                future.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (Throwable t) {
                logger.finest("Error occurred at loadAll operation execution while waiting it to finish on cache close!", t);
                handleFailureOnCompletionListener(completionListener, t);
            }
            iterator.remove();
        }
    }

    private void closeListeners() {
        deregisterAllCacheEntryListener(listenerCompleter.getListenersIds(true));
        deregisterAllCacheEntryListener(listenerCompleter.getListenersIds(false));

        listenerCompleter.clearListeners();
        for (Closeable closeable : closeableListeners.values()) {
            IOUtil.closeResource(closeable);
        }
    }

    private ClientMessage invoke(ClientMessage clientMessage, Data keyData) {
        try {
            int partitionId = getContext().getPartitionService().getPartitionId(keyData);
            Future future = new ClientInvocation(getClient(), clientMessage, getName(), partitionId).invoke();
            return (ClientMessage) future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void submitLoadAllTask(ClientMessage request, CompletionListener completionListener, final List<Data> binaryKeys) {
        final CompletionListener listener = completionListener != null ? completionListener : NULL_COMPLETION_LISTENER;
        ClientDelegatingFuture<V> delegatingFuture = null;
        try {
            injectDependencies(completionListener);

            final long startNanos = nowInNanosOrDefault();
            ClientInvocationFuture future = new ClientInvocation(getClient(), request, getName()).invoke();
            delegatingFuture = newDelegatingFuture(future, clientMessage -> Boolean.TRUE);
            final Future delFuture = delegatingFuture;
            loadAllCalls.put(delegatingFuture, listener);
            delegatingFuture.whenCompleteAsync((response, t) -> {
                if (t == null) {
                    loadAllCalls.remove(delFuture);
                    onLoadAll(binaryKeys, response, startNanos);
                    listener.onCompletion();
                } else {
                    loadAllCalls.remove(delFuture);
                    handleFailureOnCompletionListener(listener, t);
                }
            });
        } catch (Throwable t) {
            if (delegatingFuture != null) {
                loadAllCalls.remove(delegatingFuture);
            }
            handleFailureOnCompletionListener(listener, t);
        }
    }

    private <T> ClientDelegatingFuture<T> newDelegatingFuture(ClientInvocationFuture future, ClientMessageDecoder decoder) {
        return new ClientDelegatingFuture<>(future, getSerializationService(), decoder);
    }

    private  <T> ClientDelegatingFuture<T> newDelegatingFuture(ClientInvocationFuture future, ClientMessageDecoder decoder,
                                                               boolean deserializeResponse) {
        return new ClientDelegatingFuture<>(future, getSerializationService(), decoder, deserializeResponse);
    }

    private <T> ClientDelegatingFuture<T> getAndRemoveInternal(Data keyData, boolean withCompletionEvent) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheGetAndRemoveCodec.encodeRequest(nameWithPrefix, keyData, completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        return newDelegatingFuture(future, clientMessage -> CacheGetAndRemoveCodec.decodeResponse(clientMessage).response);
    }

    private ClientInvocationFuture putInternal(Data keyData, Data valueData, Data expiryPolicyData, boolean isGet,
                                               boolean withCompletionEvent) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CachePutCodec.encodeRequest(nameWithPrefix, keyData, valueData, expiryPolicyData, isGet,
                completionId);
        return invoke(request, keyData, completionId);
    }
}
