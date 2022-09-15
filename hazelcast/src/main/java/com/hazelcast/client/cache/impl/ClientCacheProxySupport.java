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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.impl.CacheEventListenerAdaptor;
import com.hazelcast.cache.impl.CacheSyncListenerCompleter;
import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.cache.impl.ClientCacheProxySupportUtil.EmptyCompletionListener;
import com.hazelcast.internal.nearcache.impl.RemoteCallHook;
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
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import javax.annotation.Nonnull;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
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

    private static final CompletionListener NULL_COMPLETION_LISTENER = new EmptyCompletionListener();

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

    ClientCacheProxySupport(CacheConfig<K, V> cacheConfig, ClientContext context) {
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

        // optimistically assume the CacheManager is already set
        if (cacheManagerRef.get() == cacheManager) {
            return;
        }

        if (!cacheManagerRef.compareAndSet(null, (HazelcastClientCacheManager) cacheManager)) {
            if (cacheManagerRef.get() == cacheManager) {
                // some other thread managed to set the same CacheManager, we are good
                return;
            }
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

    @SuppressWarnings("unchecked")
    protected <T> T injectDependencies(T obj) {
        ManagedContext managedContext = getSerializationService().getManagedContext();
        return (T) managedContext.initialize(obj);
    }

    protected long nowInNanosOrDefault() {
        return statisticsEnabled ? Timer.nanos() : -1;
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

    protected V getAndRemoveSyncInternal(K key) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        validateNotNull(key);
        validateConfiguredTypes(cacheConfig, key);

        Data keyData = toData(key);
        V response = callGetAndRemoveSync(key, keyData);
        if (statisticsEnabled) {
            statsHandler.onRemove(true, startNanos, response);
        }
        return response;
    }

    @Nonnull
    protected V callGetAndRemoveSync(K key, Data keyData) {
        try {
            return (V) getAndRemoveInternal(keyData, true).get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected <T> CompletableFuture<T> getAndRemoveAsyncInternal(K key) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        validateNotNull(key);
        validateConfiguredTypes(cacheConfig, key);

        Data keyData = toData(key);
        BiConsumer<T, Throwable> statsCallback = statisticsEnabled
                ? statsHandler.newOnRemoveCallback(true, startNanos)
                : null;
        return callGetAndRemoveAsync(key, keyData, statsCallback);
    }

    @Nonnull
    protected <T> CompletableFuture<T> callGetAndRemoveAsync(K key, Data keyData,
                                                             BiConsumer<T, Throwable> statsCallback) {
        ClientDelegatingFuture<T> delegatingFuture = getAndRemoveInternal(keyData, false);
        addCallback(delegatingFuture, statsCallback);
        return delegatingFuture;
    }

    private <T> ClientDelegatingFuture<T> getAndRemoveInternal(Data keyData, boolean withCompletionEvent) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheGetAndRemoveCodec.encodeRequest(nameWithPrefix, keyData, completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        return newDelegatingFuture(future, CacheGetAndRemoveCodec::decodeResponse);
    }

    protected boolean removeSync(K key, V oldValue,
                                 boolean hasOldValue, boolean withCompletionEvent) {
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
        boolean removed = callRemoveSync(key, keyData, oldValueData, withCompletionEvent);
        if (statisticsEnabled) {
            statsHandler.onRemove(false, startNanos, removed);
        }
        return removed;
    }

    protected boolean callRemoveSync(K key, Data keyData, Data oldValueData, boolean withCompletionEvent) {
        InternalCompletableFuture<Boolean> delegatingFuture
                = doRemoveOnServer(keyData, oldValueData, withCompletionEvent);
        try {
            return delegatingFuture.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected CompletableFuture<Boolean> removeAsync(K key, V oldValue,
                                                     boolean hasOldValue, boolean withCompletionEvent) {
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

        BiConsumer<Boolean, Throwable> callback = statisticsEnabled
                ? statsHandler.newOnRemoveCallback(false, startNanos)
                : null;
        return callRemoveAsync(key, keyData, oldValueData, withCompletionEvent, callback);
    }

    protected CompletableFuture<Boolean> callRemoveAsync(K key, Data keyData, Data oldValueData,
                                                         boolean withCompletionEvent,
                                                         BiConsumer<Boolean, Throwable> callback) {
        return addCallback(doRemoveOnServer(keyData, oldValueData, withCompletionEvent), callback);
    }

    @Nonnull
    private InternalCompletableFuture<Boolean> doRemoveOnServer(Data keyData, Data oldValueData, boolean withCompletionEvent) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheRemoveCodec.encodeRequest(nameWithPrefix, keyData, oldValueData, completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        return newDelegatingFuture(future, CacheRemoveCodec::decodeResponse);
    }

    protected boolean replaceSync(K key, V oldValue, V newValue,
                                  ExpiryPolicy expiryPolicy, boolean hasOldValue) {
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

        boolean replaced = callReplaceSync(key, keyData, newValue, newValueData,
                oldValueData, expiryPolicyData);
        if (statisticsEnabled) {
            statsHandler.onReplace(false, startNanos, replaced);
        }
        return replaced;
    }

    protected boolean callReplaceSync(K key, Data keyData, V newValue, Data newValueData,
                                      Data oldValueData, Data expiryPolicyData) {
        CompletableFuture<Boolean> future = doReplaceOnServer(keyData, newValueData,
                oldValueData, expiryPolicyData, true, null);
        try {
            return future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
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

        BiConsumer<Boolean, Throwable> statsCallback = statisticsEnabled
                ? statsHandler.newOnReplaceCallback(startNanos)
                : null;

        return callReplaceAsync(key, keyData, newValue, newValueData, oldValueData,
                expiryPolicyData, withCompletionEvent, statsCallback);
    }

    protected CompletableFuture<Boolean> callReplaceAsync(K key, Data keyData, V newValue, Data newValueData,
                                                          Data oldValueData, Data expiryPolicyData,
                                                          boolean withCompletionEvent,
                                                          BiConsumer<Boolean, Throwable> statsCallback) {
        return doReplaceOnServer(keyData, newValueData, oldValueData, expiryPolicyData, withCompletionEvent, statsCallback);
    }

    private CompletableFuture<Boolean> doReplaceOnServer(Data keyData, Data newValueData,
                                                         Data oldValueData, Data expiryPolicyData,
                                                         boolean withCompletionEvent,
                                                         BiConsumer<Boolean, Throwable> statsCallback) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheReplaceCodec.encodeRequest(nameWithPrefix, keyData, oldValueData, newValueData,
                expiryPolicyData, completionId);

        ClientDelegatingFuture<Boolean> delegatingFuture
                = newDelegatingFuture(invoke(request, keyData, completionId), CacheReplaceCodec::decodeResponse);

        return addCallback(delegatingFuture, statsCallback);
    }

    protected V getAndReplaceSync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
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

        V response = callGetAndReplaceSync(key, keyData, newValue, newValueData,
                expiryPolicyData, withCompletionEvent);

        if (statisticsEnabled) {
            statsHandler.onReplace(true, startNanos, response);
        }
        return response;
    }

    protected V callGetAndReplaceSync(K key, Data keyData, V newValue, Data newValueData,
                                      Data expiryPolicyData, boolean withCompletionEvent) {
        try {
            return (V) doGetAndReplaceOnServer(keyData, newValueData,
                    expiryPolicyData, withCompletionEvent, null).get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private <T> CompletableFuture<T> doGetAndReplaceOnServer(Data keyData, Data newValueData,
                                                             Data expiryPolicyData,
                                                             boolean withCompletionEvent,
                                                             BiConsumer<T, Throwable> statsCallback) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheGetAndReplaceCodec.encodeRequest(nameWithPrefix, keyData,
                newValueData, expiryPolicyData, completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        ClientDelegatingFuture<T> delegatingFuture
                = newDelegatingFuture(future, CacheGetAndReplaceCodec::decodeResponse);
        return addCallback(delegatingFuture, statsCallback);
    }

    protected <T> CompletableFuture<T> getAndReplaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
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

        BiConsumer<T, Throwable> statsCallback
                = statisticsEnabled
                ? statsHandler.newOnReplaceCallback(startNanos) : null;

        return callGetAndReplaceAsync(key, keyData, newValue, newValueData,
                expiryPolicyData, withCompletionEvent, statsCallback);
    }

    protected <T> CompletableFuture<T> callGetAndReplaceAsync(K key, Data keyData,
                                                              V newValue, Data newValueData,
                                                              Data expiryPolicyData,
                                                              boolean withCompletionEvent,
                                                              BiConsumer<T, Throwable> statsCallback) {
        return doGetAndReplaceOnServer(keyData, newValueData,
                expiryPolicyData, withCompletionEvent, statsCallback);
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
            V response = callPutSync(key, keyData, value, valueData, expiryPolicyData, isGet);

            if (statisticsEnabled) {
                statsHandler.onPut(isGet, startNanos, response != null);
            }
            return response;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected V callPutSync(K key, Data keyData,
                            V value, Data valueData,
                            Data expiryPolicyData, boolean isGet) throws InterruptedException, ExecutionException {
        ClientInvocationFuture invocationFuture = putInternal(keyData, valueData, expiryPolicyData, isGet, true);
        ClientDelegatingFuture<V> delegatingFuture =
                newDelegatingFuture(invocationFuture, CachePutCodec::decodeResponse);
        return delegatingFuture.get();
    }

    protected CompletableFuture putAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy,
                                                 boolean isGet,
                                                 boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        validateConfiguredTypes(cacheConfig, key, value);

        Data keyData = toData(key);
        Data valueData = toData(value);
        Data expiryPolicyData = toData(expiryPolicy);

        BiConsumer<V, Throwable> statsCallback = newStatsCallbackOrNull(isGet);
        return callPutAsync(key, keyData, value, valueData, expiryPolicyData,
                isGet, withCompletionEvent, statsCallback);
    }

    protected CompletableFuture callPutAsync(K key, Data keyData, V value,
                                             Data valueData, Data expiryPolicyData,
                                             boolean isGet, boolean withCompletionEvent,
                                             BiConsumer<V, Throwable> statsCallback) {
        ClientInvocationFuture invocationFuture = putInternal(keyData, valueData, expiryPolicyData, isGet, withCompletionEvent);
        InternalCompletableFuture future
                = newDelegatingFuture(invocationFuture, CachePutCodec::decodeResponse);

        return addCallback(future, statsCallback);
    }

    protected Boolean putIfAbsentSync(K key, V value, ExpiryPolicy expiryPolicy, boolean withCompletionEvent) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        validateNotNull(key, value);
        validateConfiguredTypes(cacheConfig, key, value);

        Data keyData = toData(key);
        Data valueData = toData(value);
        Data expiryPolicyData = toData(expiryPolicy);

        Boolean result = callPutIfAbsentSync(key, keyData, value, valueData, expiryPolicyData, withCompletionEvent);
        if (statisticsEnabled) {
            statsHandler.onPutIfAbsent(startNanos, result);
        }
        return result;
    }

    protected Boolean callPutIfAbsentSync(K key, Data keyData, V value, Data valueData,
                                          Data expiryPolicyData, boolean withCompletionEvent) {
        InternalCompletableFuture<Boolean> future = doPutIfAbsentOnServer(keyData, valueData,
                expiryPolicyData, withCompletionEvent);
        try {
            return future.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected CompletableFuture<Boolean> putIfAbsentAsync(K key, V value,
                                                          ExpiryPolicy expiryPolicy,
                                                          boolean withCompletionEvent) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        validateNotNull(key, value);
        validateConfiguredTypes(cacheConfig, key, value);

        Data keyData = toData(key);
        Data valueData = toData(value);
        Data expiryPolicyData = toData(expiryPolicy);

        BiConsumer<Boolean, Throwable> statsCallback = statisticsEnabled
                ? statsHandler.newOnPutIfAbsentCallback(startNanos)
                : null;

        return callPutIfAbsentAsync(key, keyData, value, valueData,
                expiryPolicyData, withCompletionEvent, statsCallback);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected CompletableFuture<Boolean> callPutIfAbsentAsync(K key, Data keyData, V value, Data valueData,
                                                              Data expiryPolicyData, boolean withCompletionEvent,
                                                              BiConsumer<Boolean, Throwable> statsCallback) {
        InternalCompletableFuture<Boolean> future = doPutIfAbsentOnServer(keyData, valueData,
                expiryPolicyData, withCompletionEvent);

        return addCallback(future, statsCallback);
    }

    private InternalCompletableFuture<Boolean> doPutIfAbsentOnServer(Data keyData, Data valueData,
                                                                     Data expiryPolicyData, boolean withCompletionEvent) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CachePutIfAbsentCodec.encodeRequest(nameWithPrefix, keyData,
                valueData, expiryPolicyData, completionId);
        ClientInvocationFuture future = invoke(request, keyData, completionId);
        return newDelegatingFuture(future, CachePutIfAbsentCodec::decodeResponse);
    }

    protected BiConsumer<V, Throwable> newStatsCallbackOrNull(boolean isGet) {
        if (!statisticsEnabled) {
            return null;
        }
        return statsHandler.newOnPutCallback(isGet, Timer.nanos());
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
                newDelegatingFuture(future, CacheSetExpiryPolicyCodec::decodeResponse);
        try {
            return delegatingFuture.get();
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
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

    protected V callGetSync(Object key, ExpiryPolicy expiryPolicy) {
        long startNanos = nowInNanosOrDefault();
        try {
            Data dataKey = toData(key);
            V value = getInternal(dataKey, expiryPolicy, false).get();
            if (statisticsEnabled) {
                statsHandler.onGet(startNanos, value != null);
            }
            return value;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected CompletableFuture<V> getAsyncInternal(Object key, ExpiryPolicy expiryPolicy) {
        long startNanos = nowInNanosOrDefault();
        BiConsumer<V, Throwable> statsCallback = statisticsEnabled
                ? statsHandler.newOnGetCallback(startNanos) : null;
        return callGetAsync(key, expiryPolicy, statsCallback);
    }

    @Nonnull
    protected CompletableFuture<V> callGetAsync(Object key, ExpiryPolicy expiryPolicy,
                                                BiConsumer<V, Throwable> statsCallback) {
        Data dataKey = toData(key);
        ClientDelegatingFuture<V> future = getInternal(dataKey, expiryPolicy, true);
        return addCallback(future, statsCallback);
    }

    protected void getAllInternal(Set<? extends K> keys, Collection<Data> dataKeys,
                                  ExpiryPolicy expiryPolicy, List<Object> resultingKeyValuePairs,
                                  long startNanos) {
        if (dataKeys.isEmpty()) {
            objectToDataCollection(keys, dataKeys, getSerializationService(), NULL_KEY_IS_NOT_ALLOWED);
        }
        Data expiryPolicyData = toData(expiryPolicy);

        ClientMessage request = CacheGetAllCodec.encodeRequest(nameWithPrefix, dataKeys, expiryPolicyData);
        ClientMessage responseMessage = invoke(request);

        List<Map.Entry<Data, Data>> response = CacheGetAllCodec.decodeResponse(responseMessage);
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

    protected void setExpiryPolicyInternal(Set<? extends K> keys, ExpiryPolicy policy,
                                           Set<Data> serializedKeys) {
        try {
            List<Data>[] keysByPartition = groupKeysToPartitions(keys, serializedKeys);
            setExpiryPolicyAndWaitForCompletion(keysByPartition, policy);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected void putAllInternal(Map<? extends K, ? extends V> userInputMap,
                                  ExpiryPolicy expiryPolicy,
                                  List<Map.Entry<Data, Data>>[] entriesPerPartition,
                                  long startNanos) {
        try {
            RemoteCallHook<K, V> nearCachingHook = createPutAllNearCachingHook(userInputMap.size());
            // first we fill entry set per partition
            groupDataToPartitions(userInputMap, entriesPerPartition, nearCachingHook);
            // then we invoke the operations and sync on completion of these operations
            Data expiryPolicyData = toData(expiryPolicy);
            callPutAllSync(entriesPerPartition, expiryPolicyData, nearCachingHook, startNanos);
        } catch (Exception t) {
            throw rethrow(t);
        }
    }

    // Overridden to inject hook for near cache population.
    protected RemoteCallHook<K, V> createPutAllNearCachingHook(int keySetSize) {
        return RemoteCallHook.EMPTY_HOOK;
    }

    private void groupDataToPartitions(Map<? extends K, ? extends V> userInputMap,
                                       List<Map.Entry<Data, Data>>[] entriesPerPartition,
                                       RemoteCallHook nearCachingHook) {
        ClientPartitionService partitionService = getContext().getPartitionService();

        for (Map.Entry<? extends K, ? extends V> entry : userInputMap.entrySet()) {
            K key = entry.getKey();
            V value = entry.getValue();
            validateNotNull(key, value);

            Data keyData = toData(key);
            Data valueData = toData(value);

            nearCachingHook.beforeRemoteCall(key, keyData, value, valueData);

            int partitionId = partitionService.getPartitionId(keyData);
            List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
            if (entries == null) {
                entries = new ArrayList<>();
                entriesPerPartition[partitionId] = entries;
            }
            // TODO Can we do this without creating SimpleImmutableEntry?
            entries.add(new AbstractMap.SimpleImmutableEntry<>(keyData, valueData));
        }
    }

    protected void callPutAllSync(List<Map.Entry<Data, Data>>[] entriesPerPartition,
                                  Data expiryPolicyData,
                                  RemoteCallHook<K, V> nearCachingHook, long startNanos) {

        List<ClientCacheProxySupportUtil.FutureEntriesTuple> futureEntriesTuples
                = new ArrayList<>(entriesPerPartition.length);

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

    protected boolean containsKeyInternal(Object key) {
        Data keyData = toData(key);
        ClientMessage request = CacheContainsKeyCodec.encodeRequest(nameWithPrefix, keyData);
        ClientMessage result = invoke(request, keyData);
        return CacheContainsKeyCodec.decodeResponse(result);
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

    private void submitLoadAllTask(ClientMessage request, CompletionListener completionListener, final List<Data> binaryKeys) {
        final CompletionListener listener = completionListener != null
            ? injectDependencies(completionListener)
            : NULL_COMPLETION_LISTENER;
        ClientDelegatingFuture<V> delegatingFuture = null;
        try {
            final long startNanos = nowInNanosOrDefault();
            ClientInvocationFuture future = new ClientInvocation(getClient(), request, getName()).invoke();
            delegatingFuture = newDelegatingFuture(future, clientMessage -> Boolean.TRUE);
            final Future delFuture = delegatingFuture;
            loadAllCalls.put(delegatingFuture, listener);
            delegatingFuture.whenCompleteAsync((response, t) -> {
                if (t == null) {
                    loadAllCalls.remove(delFuture);
                    onLoadAll(binaryKeys, startNanos);
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

    private void onLoadAll(List<Data> keys, long startNanos) {
        if (statisticsEnabled) {
            statsHandler.onBatchPut(startNanos, keys.size());
        }
    }

    protected Object invokeInternal(Object key, Data epData, Object... arguments) {
        Data keyData = toData(key);
        List<Data> argumentsData;
        if (arguments != null) {
            argumentsData = new ArrayList<>(arguments.length);
            for (Object argument : arguments) {
                argumentsData.add(toData(argument));
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
            Data data = CacheEntryProcessorCodec.decodeResponse(response);
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
                UUID uuid = member.getUuid();
                Data configData = toData(cacheEntryListenerConfiguration);
                ClientMessage request = CacheListenerRegistrationCodec.encodeRequest(nameWithPrefix, configData, isRegister,
                        uuid);
                ClientInvocation invocation = new ClientInvocation(getClient(), request, getName(), uuid);
                invocation.invoke();
            } catch (Exception e) {
                sneakyThrow(e);
            }
        }
    }

    protected ILogger getLogger() {
        return logger;
    }

    private ClientDelegatingFuture<V> getInternal(Data keyData, ExpiryPolicy expiryPolicy, boolean deserializeResponse) {
        Data expiryPolicyData = toData(expiryPolicy);

        ClientMessage request = CacheGetCodec.encodeRequest(nameWithPrefix, keyData, expiryPolicyData);
        int partitionId = getContext().getPartitionService().getPartitionId(keyData);

        ClientInvocation clientInvocation = new ClientInvocation(getClient(), request, name, partitionId);
        ClientInvocationFuture future = clientInvocation.invoke();
        return newDelegatingFuture(future, CacheGetCodec::decodeResponse, deserializeResponse);
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
                partition = new ArrayList<>();
                keysByPartition[partitionId] = partition;
            }
            partition.add(keyData);
        }
        return keysByPartition;
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

    private <T> ClientDelegatingFuture<T> newDelegatingFuture(ClientInvocationFuture future, ClientMessageDecoder decoder) {
        return new ClientDelegatingFuture<>(future, getSerializationService(), decoder);
    }

    private <T> ClientDelegatingFuture<T> newDelegatingFuture(ClientInvocationFuture future, ClientMessageDecoder decoder,
                                                              boolean deserializeResponse) {
        return new ClientDelegatingFuture<>(future, getSerializationService(), decoder, deserializeResponse);
    }

    public ClientInvocationFuture putInternal(Data keyData, Data valueData, Data expiryPolicyData, boolean isGet,
                                              boolean withCompletionEvent) {
        int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CachePutCodec.encodeRequest(nameWithPrefix, keyData, valueData, expiryPolicyData, isGet,
                completionId);
        return invoke(request, keyData, completionId);
    }
}
