/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.CacheClearResponse;
import com.hazelcast.cache.impl.CacheEventListenerAdaptor;
import com.hazelcast.cache.impl.CacheProxyUtil;
import com.hazelcast.cache.impl.CacheSyncListenerCompleter;
import com.hazelcast.cache.impl.client.CacheAddInvalidationListenerRequest;
import com.hazelcast.cache.impl.client.CacheBatchInvalidationMessage;
import com.hazelcast.cache.impl.client.CacheClearRequest;
import com.hazelcast.cache.impl.client.CacheGetAndRemoveRequest;
import com.hazelcast.cache.impl.client.CacheGetAndReplaceRequest;
import com.hazelcast.cache.impl.client.CacheInvalidationMessage;
import com.hazelcast.cache.impl.client.CachePutIfAbsentRequest;
import com.hazelcast.cache.impl.client.CachePutRequest;
import com.hazelcast.cache.impl.client.CacheRemoveInvalidationListenerRequest;
import com.hazelcast.cache.impl.client.CacheRemoveRequest;
import com.hazelcast.cache.impl.client.CacheReplaceRequest;
import com.hazelcast.cache.impl.client.CacheSingleInvalidationMessage;
import com.hazelcast.cache.impl.client.CompletionAwareCacheRequest;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheExecutor;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.CompletedFuture;
import com.hazelcast.util.executor.DelegatingFuture;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;

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
abstract class AbstractClientInternalCacheProxy<K, V>
        extends AbstractClientCacheProxyBase<K, V> implements CacheSyncListenerCompleter {

    private static final long MAX_COMPLETION_LATCH_WAIT_TIME = TimeUnit.MINUTES.toMillis(5);
    private static final long COMPLETION_LATCH_WAIT_TIME_STEP = TimeUnit.SECONDS.toMillis(1);

    protected final ILogger logger = Logger.getLogger(getClass());

    protected final HazelcastClientCacheManager cacheManager;
    protected final NearCacheManager nearCacheManager;
    // Object => Data or <V>
    protected NearCache<Data, Object> nearCache;

    protected String nearCacheMembershipRegistrationId;

    protected final ClientCacheStatisticsImpl statistics;
    protected final boolean statisticsEnabled;

    protected boolean cacheOnUpdate;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations;
    private final ConcurrentMap<Integer, CountDownLatch> syncLocks;

    private final AtomicInteger completionIdCounter = new AtomicInteger();

    protected AbstractClientInternalCacheProxy(CacheConfig cacheConfig, ClientContext clientContext,
                                               HazelcastClientCacheManager cacheManager) {
        super(cacheConfig, clientContext);
        this.cacheManager = cacheManager;
        this.nearCacheManager = clientContext.getNearCacheManager();
        this.asyncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        this.syncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        this.syncLocks = new ConcurrentHashMap<Integer, CountDownLatch>();

        initNearCache();

        if (nearCache != null) {
            this.statistics = new ClientCacheStatisticsImpl(System.currentTimeMillis(),
                    nearCache.getNearCacheStats());
        } else {
            this.statistics = new ClientCacheStatisticsImpl(System.currentTimeMillis());
        }
        this.statisticsEnabled = cacheConfig.isStatisticsEnabled();
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

    private static class ClientNearCacheExecutor implements NearCacheExecutor {

        private ClientExecutionService clientExecutionService;

        private ClientNearCacheExecutor(ClientExecutionService clientExecutionService) {
            this.clientExecutionService = clientExecutionService;
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
                                                         long delay, TimeUnit unit) {
            return clientExecutionService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
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
    public void destroy() {
        if (nearCache != null) {
            removeInvalidationListener();
            nearCacheManager.destroyNearCache(nearCache.getName());
        }
        if (statisticsEnabled) {
            statistics.clear();
        }
        super.destroy();
    }

    protected <T> ClientInvocationFuture<T> invoke(ClientRequest req, int partitionId, boolean completionOperation) {
        Integer completionId = null;
        if (completionOperation) {
            completionId = registerCompletionLatch(1);
            if (req instanceof CompletionAwareCacheRequest) {
                ((CompletionAwareCacheRequest) req).setCompletionId(completionId);
            }
        }
        try {
            HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
            final ClientInvocation clientInvocation = new ClientInvocation(client, req, partitionId);
            final ClientInvocationFuture<T> f = clientInvocation.invoke();
            f.setResponseDeserialized(true);
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
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected <T> ClientInvocationFuture<T> invoke(ClientRequest req, Data keyData, boolean completionOperation) {
        int partitionId = clientContext.getPartitionService().getPartitionId(keyData);
        return invoke(req, partitionId, completionOperation);
    }

    protected <T> T getSafely(Future<T> future) {
        try {
            return future.get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    protected <T> ICompletableFuture<T> removeAsyncInternal(K key, V oldValue, final boolean hasOldValue,
                                                            final boolean isGet, boolean withCompletionEvent,
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
        final Data oldValueData = oldValue != null ? toData(oldValue) : null;
        ClientRequest request;
        InMemoryFormat inMemoryFormat = cacheConfig.getInMemoryFormat();
        if (isGet) {
            request = new CacheGetAndRemoveRequest(nameWithPrefix, keyData, inMemoryFormat);
        } else {
            request = new CacheRemoveRequest(nameWithPrefix, keyData, oldValueData, inMemoryFormat);
        }

        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, withCompletionEvent);

            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        DelegatingFuture delegatingFuture = new DelegatingFuture<T>(future, clientContext.getSerializationService());
        if (async && statisticsEnabled) {
            delegatingFuture.andThen(new ExecutionCallback<Object>() {
                public void onResponse(Object responseData) {
                    Object response = clientContext.getSerializationService().toObject(responseData);
                    handleStatisticsOnRemove(isGet, start, response);
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

    protected <T> ICompletableFuture<T> replaceAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                             boolean hasOldValue, final boolean isGet,
                                                             boolean withCompletionEvent, boolean async) {
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
        final Data oldValueData = oldValue != null ? toData(oldValue) : null;
        final Data newValueData = newValue != null ? toData(newValue) : null;
        InMemoryFormat inMemoryFormat = cacheConfig.getInMemoryFormat();
        ClientRequest request;
        if (isGet) {
            request = new CacheGetAndReplaceRequest(nameWithPrefix, keyData, newValueData,
                    expiryPolicy, inMemoryFormat);
        } else {
            request = new CacheReplaceRequest(nameWithPrefix, keyData, oldValueData, newValueData,
                    expiryPolicy, inMemoryFormat);
        }
        ICompletableFuture future;
        try {
            future = invoke(request, keyData, withCompletionEvent);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        DelegatingFuture delegatingFuture = new DelegatingFuture<T>(future, clientContext.getSerializationService());
        if (async && statisticsEnabled) {
            delegatingFuture.andThen(new ExecutionCallback<Object>() {
                public void onResponse(Object responseData) {
                    Object response = clientContext.getSerializationService().toObject(responseData);
                    handleStatisticsOnReplace(isGet, start, response);
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

    protected Object putInternal(final K key, final V value, final ExpiryPolicy expiryPolicy,
                                 final boolean isGet, final boolean withCompletionEvent,
                                 final boolean async) {
        final long start = System.nanoTime();
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        InMemoryFormat inMemoryFormat = cacheConfig.getInMemoryFormat();
        CachePutRequest request = new CachePutRequest(nameWithPrefix, keyData, valueData,
                expiryPolicy, isGet, inMemoryFormat);
        ICompletableFuture future;
        try {
            future = invoke(request, keyData, withCompletionEvent);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        if (!async) {
            try {
                Object response = future.get();
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
                throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
            }
        } else {
            if (nearCache != null || statisticsEnabled) {
                future.andThen(new ExecutionCallback<Object>() {
                    @Override
                    public void onResponse(Object responseData) {
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
                    public void onFailure(Throwable t) {

                    }
                });
            }
            return future;
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

    protected Object putIfAbsentInternal(final K key, final V value, final ExpiryPolicy expiryPolicy,
                                         final boolean withCompletionEvent, final boolean async) {
        final long start = System.nanoTime();
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CachePutIfAbsentRequest request = new CachePutIfAbsentRequest(nameWithPrefix, keyData, valueData,
                expiryPolicy, cacheConfig.getInMemoryFormat());
        ICompletableFuture<Boolean> future;
        try {
            future = invoke(request, keyData, withCompletionEvent);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        DelegatingFuture delegatingFuture =
                new DelegatingFuture<Boolean>(future, clientContext.getSerializationService());
        if (!async) {
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
                throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
            }
        } else {
            if (nearCache != null || (async && statisticsEnabled)) {
                delegatingFuture.andThen(new ExecutionCallback<Object>() {
                    @Override
                    public void onResponse(Object responseData) {
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
    }

    protected void handleStatisticsOnPutIfAbsent(long start, boolean saved) {
        if (saved) {
            statistics.increaseCachePuts();
            statistics.addPutTimeNanos(System.nanoTime() - start);
        }
    }

    protected void removeAllInternal(Set<? extends K> keys) {
        final long start = System.nanoTime();
        final Set<Data> keysData;
        if (keys != null) {
            keysData = new HashSet<Data>();
            for (K key : keys) {
                keysData.add(toData(key));
            }
        } else {
            keysData = null;
        }
        final int partitionCount = clientContext.getPartitionService().getPartitionCount();
        int completionId = registerCompletionLatch(partitionCount);
        CacheClearRequest request = new CacheClearRequest(nameWithPrefix, keysData, true, completionId);
        try {
            final Map<Integer, Object> results = invoke(request);
            int completionCount = 0;
            for (Object result : results.values()) {
                if (result != null && result instanceof CacheClearResponse) {
                    final Object response = ((CacheClearResponse) result).getResponse();
                    if (response instanceof Boolean) {
                        completionCount++;
                    }
                    if (response instanceof Throwable) {
                        throw (Throwable) response;
                    }
                }
            }
            waitCompletionLatch(completionId, partitionCount - completionCount, null);
            if (statisticsEnabled) {
                if (keysData != null) {
                    // Actually we don't know how many of them are really removed or not.
                    // We just assume that if there is no exception, all of them are removed.
                    // Otherwise (if there is an exception), we don't update any cache stats about remove.
                    statistics.increaseCacheRemovals(keysData.size());
                    statistics.addRemoveTimeNanos(System.nanoTime() - start);
                } else {
                    statistics.setLastUpdateTime(System.currentTimeMillis());
                    // We don't support count stats of removing all entries.
                }
            }
        } catch (Throwable t) {
            deregisterCompletionLatch(completionId);
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    protected void clearInternal() {
        CacheClearRequest request = new CacheClearRequest(nameWithPrefix, null, false, -1);
        try {
            final Map<Integer, Object> results = invoke(request);
            for (Object result : results.values()) {
                if (result != null && result instanceof CacheClearResponse) {
                    final Object response = ((CacheClearResponse) result).getResponse();
                    if (response instanceof Throwable) {
                        throw (Throwable) response;
                    }
                }
            }
            if (statisticsEnabled) {
                statistics.setLastUpdateTime(System.currentTimeMillis());
                // We don't support count stats of removing all entries.
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
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

    protected void addListenerLocally(String regId,
                                      CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
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
        for (String regId : listenerRegistrations) {
            clientContext.getListenerService().deregisterListener(regId);
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

    protected Integer registerCompletionLatch(int count) {
        if (!syncListenerRegistrations.isEmpty()) {
            final int id = completionIdCounter.incrementAndGet();
            int size = syncListenerRegistrations.size();
            CountDownLatch countDownLatch = new CountDownLatch(count * size);
            syncLocks.put(id, countDownLatch);
            return id;
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

    protected void waitCompletionLatch(Integer countDownLatchId, int offset, ICompletableFuture future)
            throws ExecutionException {
        if (countDownLatchId != IGNORE_COMPLETION) {
            final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
            if (countDownLatch != null) {
                for (int i = 0; i < offset; i++) {
                    countDownLatch.countDown();
                }
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

    protected EventHandler<Object> createHandler(final CacheEventListenerAdaptor adaptor) {
        return new EventHandler<Object>() {
            @Override
            public void handle(Object event) {
                adaptor.handleEvent(event);
            }

            @Override
            public void beforeListenerRegister() {

            }

            @Override
            public void onListenerRegister() {

            }
        };
    }

    protected ICompletableFuture createCompletedFuture(Object value) {
        return new CompletedFuture(clientContext.getSerializationService(), value,
                clientContext.getExecutionService().getAsyncExecutor());
    }

    private class NearCacheInvalidationHandler implements EventHandler<CacheInvalidationMessage> {

        private final Client client;

        private NearCacheInvalidationHandler(Client client) {
            this.client = client;
        }

        @Override
        public void handle(CacheInvalidationMessage message) {
            if (client.getUuid().equals(message.getSourceUuid())) {
                return;
            }
            if (message instanceof CacheSingleInvalidationMessage) {
                CacheSingleInvalidationMessage singleInvalidationMessage =
                        (CacheSingleInvalidationMessage) message;
                Data key = singleInvalidationMessage.getKey();
                if (key != null) {
                    nearCache.remove(key);
                } else {
                    nearCache.clear();
                }
            } else if (message instanceof CacheBatchInvalidationMessage) {
                CacheBatchInvalidationMessage batchInvalidationMessage =
                        (CacheBatchInvalidationMessage) message;
                List<CacheSingleInvalidationMessage> invalidationMessages =
                        batchInvalidationMessage.getInvalidationMessages();
                if (invalidationMessages != null) {
                    for (CacheSingleInvalidationMessage invalidationMessage : invalidationMessages) {
                        if (!client.getUuid().equals(invalidationMessage.getSourceUuid())) {
                            nearCache.remove(invalidationMessage.getKey());
                        }
                    }
                }
            } else {
                logger.finest("Unknown invalidation message: " + message);
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
            ClientListenerService listenerService = clientContext.getListenerService();
            CacheAddInvalidationListenerRequest addRequest = new CacheAddInvalidationListenerRequest(nameWithPrefix);
            Client client = clientContext.getClusterService().getLocalClient();
            EventHandler handler = new NearCacheInvalidationHandler(client);
            BaseClientRemoveListenerRequest removeRequest =
                    new CacheRemoveInvalidationListenerRequest(nameWithPrefix);
            nearCacheMembershipRegistrationId = listenerService.registerListener(addRequest, removeRequest, handler);
        }
    }

    private void removeInvalidationListener() {
        if (nearCache != null && nearCache.isInvalidateOnChange()) {
            String registrationId = nearCacheMembershipRegistrationId;
            if (registrationId != null) {
                ClientListenerService listenerService = clientContext.getListenerService();
                listenerService.deregisterListener(nearCacheMembershipRegistrationId);
            }
        }
    }


}
