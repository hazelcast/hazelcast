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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.impl.event.CachePartitionLostEventFilter;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.cache.impl.event.InternalCachePartitionLostListenerAdapter;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;
import java.util.EventListener;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.util.ExceptionUtil.rethrowAllowedTypeFirst;
import static com.hazelcast.util.SetUtil.createHashSet;
import static java.lang.Thread.currentThread;

/**
 * Abstract {@link com.hazelcast.cache.ICache} implementation which provides shared internal implementations
 * of cache operations like put, replace, remove and invoke. These internal implementations are delegated
 * by actual cache methods.
 * <p/>
 * <p>Note: this partial implementation is used by server or embedded mode cache.</p>
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see com.hazelcast.cache.impl.CacheProxy
 * @see com.hazelcast.cache.ICache
 */
abstract class AbstractInternalCacheProxy<K, V>
        extends AbstractCacheProxyBase<K, V>
        implements CacheSyncListenerCompleter {

    private static final long MAX_COMPLETION_LATCH_WAIT_TIME = TimeUnit.MINUTES.toMillis(5);
    private static final long COMPLETION_LATCH_WAIT_TIME_STEP = TimeUnit.SECONDS.toMillis(1);

    private final AtomicInteger completionIdCounter = new AtomicInteger();

    private final ConcurrentMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations;
    private final ConcurrentMap<Integer, CountDownLatch> syncLocks;

    private AtomicReference<HazelcastServerCacheManager> cacheManagerRef = new AtomicReference<HazelcastServerCacheManager>();

    AbstractInternalCacheProxy(CacheConfig<K, V> cacheConfig, NodeEngine nodeEngine, ICacheService cacheService) {
        super(cacheConfig, nodeEngine, cacheService);
        asyncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncLocks = new ConcurrentHashMap<Integer, CountDownLatch>();

        List<CachePartitionLostListenerConfig> configs = cacheConfig.getPartitionLostListenerConfigs();
        for (CachePartitionLostListenerConfig listenerConfig : configs) {
            CachePartitionLostListener listener = initializeListener(listenerConfig);
            if (listener != null) {
                EventFilter filter = new CachePartitionLostEventFilter();
                CacheEventListener listenerAdapter = new InternalCachePartitionLostListenerAdapter(listener);
                getService().getNodeEngine().getEventService()
                        .registerListener(AbstractCacheService.SERVICE_NAME, name, filter, listenerAdapter);
            }
        }
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManagerRef.get();
    }

    @Override
    public void setCacheManager(HazelcastCacheManager cacheManager) {
        assert cacheManager instanceof HazelcastServerCacheManager;

        if (cacheManagerRef.get() == cacheManager) {
            return;
        }

        if (!this.cacheManagerRef.compareAndSet(null, (HazelcastServerCacheManager) cacheManager)) {
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

    @Override
    protected void closeListeners() {
        deregisterAllCacheEntryListener(syncListenerRegistrations.values());
        deregisterAllCacheEntryListener(asyncListenerRegistrations.values());

        syncListenerRegistrations.clear();
        asyncListenerRegistrations.clear();
        notifyAndClearSyncListenerLatches();
    }

    @Override
    public CacheStatistics getLocalCacheStatistics() {
        // TODO: throw UnsupportedOperationException if cache statistics are not enabled (but it breaks backward compatibility)
        return getService().createCacheStatIfAbsent(cacheConfig.getNameWithPrefix());
    }

    <T> InternalCompletableFuture<T> invoke(Operation op, int partitionId, boolean completionOperation) {
        Integer completionId = null;
        if (completionOperation) {
            completionId = registerCompletionLatch(1);
            if (op instanceof MutableOperation) {
                ((MutableOperation) op).setCompletionId(completionId);
            }
        }
        try {
            InternalCompletableFuture<T> future = getNodeEngine().getOperationService()
                    .invokeOnPartition(getServiceName(), op, partitionId);
            if (completionOperation) {
                waitCompletionLatch(completionId);
            }
            return future;
        } catch (Throwable e) {
            if (e instanceof IllegalStateException) {
                close();
            }
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        } finally {
            if (completionOperation) {
                deregisterCompletionLatch(completionId);
            }
        }
    }

    <T> InternalCompletableFuture<T> invoke(Operation op, Data keyData, boolean completionOperation) {
        int partitionId = getPartitionId(keyData);
        return invoke(op, partitionId, completionOperation);
    }

    <T> InternalCompletableFuture<T> removeAsyncInternal(K key, V oldValue, boolean hasOldValue,
                                                         boolean isGet, boolean withCompletionEvent) {
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue);
        } else {
            validateNotNull(key);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key);
        }
        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(oldValue);
        Operation operation;
        if (isGet) {
            operation = operationProvider.createGetAndRemoveOperation(keyData, IGNORE_COMPLETION);
        } else {
            operation = operationProvider.createRemoveOperation(keyData, valueData, IGNORE_COMPLETION);
        }
        return invoke(operation, keyData, withCompletionEvent);
    }

    <T> InternalCompletableFuture<T> replaceAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                          boolean hasOldValue, boolean isGet, boolean withCompletionEvent) {
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, newValue);
        }
        Data keyData = serializationService.toData(key);
        Data oldValueData = serializationService.toData(oldValue);
        Data newValueData = serializationService.toData(newValue);
        Operation operation;
        if (isGet) {
            operation = operationProvider.createGetAndReplaceOperation(keyData, newValueData, expiryPolicy, IGNORE_COMPLETION);
        } else {
            operation = operationProvider.createReplaceOperation(keyData, oldValueData, newValueData, expiryPolicy,
                    IGNORE_COMPLETION);
        }
        return invoke(operation, keyData, withCompletionEvent);
    }

    <T> InternalCompletableFuture<T> putAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy,
                                                      boolean isGet, boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(value);
        Operation op = operationProvider.createPutOperation(keyData, valueData, expiryPolicy, isGet, IGNORE_COMPLETION);
        return invoke(op, keyData, withCompletionEvent);
    }

    InternalCompletableFuture<Boolean> putIfAbsentAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy,
                                                                boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(value);
        Operation operation = operationProvider.createPutIfAbsentOperation(keyData, valueData, expiryPolicy, IGNORE_COMPLETION);
        return invoke(operation, keyData, withCompletionEvent);
    }

    void clearInternal() {
        try {
            OperationService operationService = getNodeEngine().getOperationService();
            OperationFactory operationFactory = operationProvider.createClearOperationFactory();
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(getServiceName(), operationFactory);
            for (Object result : results.values()) {
                if (result != null && result instanceof CacheClearResponse) {
                    Object response = ((CacheClearResponse) result).getResponse();
                    if (response instanceof Throwable) {
                        throw (Throwable) response;
                    }
                }
            }
        } catch (Throwable t) {
            throw rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    void removeAllInternal(Set<? extends K> keys) {
        Set<Data> keysData = null;
        if (keys != null) {
            keysData = createHashSet(keys.size());
            for (K key : keys) {
                validateNotNull(key);
                keysData.add(serializationService.toData(key));
            }
        }
        int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();
        Integer completionId = registerCompletionLatch(partitionCount);
        OperationService operationService = getNodeEngine().getOperationService();
        OperationFactory operationFactory = operationProvider.createRemoveAllOperationFactory(keysData, completionId);
        try {
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(getServiceName(), operationFactory);
            int completionCount = 0;
            for (Object result : results.values()) {
                if (result != null && result instanceof CacheClearResponse) {
                    Object response = ((CacheClearResponse) result).getResponse();
                    if (response instanceof Boolean) {
                        completionCount++;
                    }
                    if (response instanceof Throwable) {
                        throw (Throwable) response;
                    }
                }
            }
            waitCompletionLatch(completionId, partitionCount - completionCount);
        } catch (Throwable t) {
            deregisterCompletionLatch(completionId);
            throw rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    void addListenerLocally(String regId, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        if (cacheEntryListenerConfiguration.isSynchronous()) {
            syncListenerRegistrations.putIfAbsent(cacheEntryListenerConfiguration, regId);
        } else {
            asyncListenerRegistrations.putIfAbsent(cacheEntryListenerConfiguration, regId);
        }
    }

    String removeListenerLocally(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        ConcurrentMap<CacheEntryListenerConfiguration, String> regs;
        if (cacheEntryListenerConfiguration.isSynchronous()) {
            regs = syncListenerRegistrations;
        } else {
            regs = asyncListenerRegistrations;
        }
        return regs.remove(cacheEntryListenerConfiguration);
    }

    String getListenerIdLocal(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        ConcurrentMap<CacheEntryListenerConfiguration, String> regs;
        if (cacheEntryListenerConfiguration.isSynchronous()) {
            regs = syncListenerRegistrations;
        } else {
            regs = asyncListenerRegistrations;
        }
        return regs.get(cacheEntryListenerConfiguration);
    }

    private void deregisterAllCacheEntryListener(Collection<String> listenerRegistrations) {
        ICacheService service = getService();
        for (String regId : listenerRegistrations) {
            service.deregisterListener(nameWithPrefix, regId);
        }
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

    Integer registerCompletionLatch(int count) {
        if (!syncListenerRegistrations.isEmpty()) {
            int id = completionIdCounter.incrementAndGet();
            int size = syncListenerRegistrations.size();
            CountDownLatch countDownLatch = new CountDownLatch(count * size);
            syncLocks.put(id, countDownLatch);
            return id;
        }
        return IGNORE_COMPLETION;
    }

    void deregisterCompletionLatch(Integer countDownLatchId) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            syncLocks.remove(countDownLatchId);
        }
    }

    void waitCompletionLatch(Integer countDownLatchId) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
            if (countDownLatch != null) {
                awaitLatch(countDownLatch);
            }
        }
    }

    private void waitCompletionLatch(Integer countDownLatchId, int offset) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
            if (countDownLatch != null) {
                for (int i = 0; i < offset; i++) {
                    countDownLatch.countDown();
                }
                awaitLatch(countDownLatch);
            }
        }
    }

    private void awaitLatch(CountDownLatch countDownLatch) {
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
                currentTimeoutMs -= COMPLETION_LATCH_WAIT_TIME_STEP;
                if (!getNodeEngine().isRunning()) {
                    throw new HazelcastInstanceNotActiveException();
                } else if (isClosed()) {
                    throw new IllegalStateException("Cache (" + nameWithPrefix + ") is closed!");
                } else if (isDestroyed()) {
                    throw new IllegalStateException("Cache (" + nameWithPrefix + ") is destroyed!");
                }

            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            ExceptionUtil.sneakyThrow(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends EventListener> T initializeListener(ListenerConfig listenerConfig) {
        T listener = null;
        if (listenerConfig.getImplementation() != null) {
            listener = (T) listenerConfig.getImplementation();
        } else if (listenerConfig.getClassName() != null) {
            try {
                listener = ClassLoaderUtil.newInstance(getNodeEngine().getConfigClassLoader(),
                        listenerConfig.getClassName());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        injectDependencies(listener);
        return listener;
    }
}
