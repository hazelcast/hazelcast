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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheStatistics;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;

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
        implements ICacheInternal<K, V>, CacheSyncListenerCompleter {

    private static final long MAX_COMPLETION_LATCH_WAIT_TIME = TimeUnit.MINUTES.toMillis(5);

    private static final long COMPLETION_LATCH_WAIT_TIME_STEP = TimeUnit.SECONDS.toMillis(1);
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations;

    private final ConcurrentMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations;
    private final ConcurrentMap<Integer, CountDownLatch> syncLocks;

    private final AtomicInteger completionIdCounter = new AtomicInteger();

    private HazelcastServerCacheManager cacheManager;

    protected AbstractInternalCacheProxy(CacheConfig cacheConfig, NodeEngine nodeEngine,
                                         ICacheService cacheService) {
        super(cacheConfig, nodeEngine, cacheService);
        asyncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncLocks = new ConcurrentHashMap<Integer, CountDownLatch>();

        final List<CachePartitionLostListenerConfig> configs = cacheConfig.getPartitionLostListenerConfigs();
        for (CachePartitionLostListenerConfig listenerConfig : configs) {
            final CachePartitionLostListener listener = initializeListener(listenerConfig);
            if (listener != null) {
                final InternalCachePartitionLostListenerAdapter listenerAdapter =
                        new InternalCachePartitionLostListenerAdapter(listener);
                final EventFilter filter = new CachePartitionLostEventFilter();
                final ICacheService service = getService();
                service.getNodeEngine().getEventService()
                        .registerListener(AbstractCacheService.SERVICE_NAME, name, filter, listenerAdapter);
            }
        }
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    void setCacheManager(HazelcastServerCacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    protected void postDestroy() {
        if (cacheManager != null) {
            cacheManager.destroyCache(getName());
        }
    }

    protected <T> InternalCompletableFuture<T> invoke(Operation op, int partitionId, boolean completionOperation) {
        Integer completionId = null;
        if (completionOperation) {
            completionId = registerCompletionLatch(1);
            if (op instanceof MutableOperation) {
                ((MutableOperation) op).setCompletionId(completionId);
            }
        }
        try {
            final InternalCompletableFuture<T> f =
                    getNodeEngine().getOperationService()
                            .invokeOnPartition(getServiceName(), op, partitionId);
            if (completionOperation) {
                waitCompletionLatch(completionId);
            }
            return f;
        } catch (Throwable e) {
            if (e instanceof IllegalStateException) {
                close();
            }
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        } finally {
            if (completionOperation) {
                deregisterCompletionLatch(completionId);
            }
        }
    }

    protected <T> InternalCompletableFuture<T> invoke(Operation op, Data keyData, boolean completionOperation) {
        final int partitionId = getPartitionId(keyData);
        return invoke(op, partitionId, completionOperation);
    }

    protected <T> InternalCompletableFuture<T> removeAsyncInternal(K key, V oldValue, boolean hasOldValue,
                                                                   boolean isGet, boolean withCompletionEvent) {
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue);
        } else {
            validateNotNull(key);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key);
        }
        final Data keyData = serializationService.toData(key);
        final Data valueData = serializationService.toData(oldValue);
        final Operation operation;
        if (isGet) {
            operation = operationProvider.createGetAndRemoveOperation(keyData, IGNORE_COMPLETION);
        } else {
            operation = operationProvider.createRemoveOperation(keyData, valueData, IGNORE_COMPLETION);
        }
        return invoke(operation, keyData, withCompletionEvent);
    }

    protected <T> InternalCompletableFuture<T> replaceAsyncInternal(K key, V oldValue, V newValue,
                                                                    ExpiryPolicy expiryPolicy,
                                                                    boolean hasOldValue, boolean isGet,
                                                                    boolean withCompletionEvent) {
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, newValue);
        }
        final Data keyData = serializationService.toData(key);
        final Data oldValueData = serializationService.toData(oldValue);
        final Data newValueData = serializationService.toData(newValue);
        final Operation operation;
        if (isGet) {
            operation = operationProvider.createGetAndReplaceOperation(keyData, newValueData,
                    expiryPolicy, IGNORE_COMPLETION);
        } else {
            operation = operationProvider.createReplaceOperation(keyData, oldValueData, newValueData,
                    expiryPolicy, IGNORE_COMPLETION);
        }
        return invoke(operation, keyData, withCompletionEvent);
    }

    protected <T> InternalCompletableFuture<T> putAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy,
                                                                boolean isGet, boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        final Data keyData = serializationService.toData(key);
        final Data valueData = serializationService.toData(value);
        final Operation op = operationProvider.createPutOperation(keyData, valueData, expiryPolicy,
                isGet, IGNORE_COMPLETION);
        return invoke(op, keyData, withCompletionEvent);
    }

    protected InternalCompletableFuture<Boolean> putIfAbsentAsyncInternal(K key, V value,
                                                                          ExpiryPolicy expiryPolicy,
                                                                          boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        final Data keyData = serializationService.toData(key);
        final Data valueData = serializationService.toData(value);
        Operation operation = operationProvider.createPutIfAbsentOperation(keyData, valueData,
                expiryPolicy, IGNORE_COMPLETION);
        return invoke(operation, keyData, withCompletionEvent);
    }

    protected void clearInternal() {
        final OperationService operationService = getNodeEngine().getOperationService();
        OperationFactory operationFactory = operationProvider.createClearOperationFactory();
        try {
            final Map<Integer, Object> results =
                    operationService.invokeOnAllPartitions(getServiceName(), operationFactory);
            for (Object result : results.values()) {
                if (result != null && result instanceof CacheClearResponse) {
                    final Object response = ((CacheClearResponse) result).getResponse();
                    if (response instanceof Throwable) {
                        throw (Throwable) response;
                    }
                }
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    protected void removeAllInternal(Set<? extends K> keys) {
        final Set<Data> keysData;
        if (keys != null) {
            keysData = new HashSet<Data>();
            for (K key : keys) {
                keysData.add(serializationService.toData(key));
            }
        } else {
            keysData = null;
        }
        final int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();
        final Integer completionId = registerCompletionLatch(partitionCount);
        final OperationService operationService = getNodeEngine().getOperationService();
        OperationFactory operationFactory =
                operationProvider.createRemoveAllOperationFactory(keysData, completionId);
        try {
            final Map<Integer, Object> results =
                    operationService.invokeOnAllPartitions(getServiceName(), operationFactory);
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
            waitCompletionLatch(completionId, partitionCount - completionCount);
        } catch (Throwable t) {
            deregisterCompletionLatch(completionId);
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
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
        final ICacheService service = getService();
        for (String regId : listenerRegistrations) {
            service.deregisterListener(nameWithPrefix, regId);
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
        return IGNORE_COMPLETION;
    }

    protected void deregisterCompletionLatch(Integer countDownLatchId) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            syncLocks.remove(countDownLatchId);
        }
    }

    protected void waitCompletionLatch(Integer countDownLatchId) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
            if (countDownLatch != null) {
                awaitLatch(countDownLatch);
            }
        }
    }

    protected void waitCompletionLatch(Integer countDownLatchId, int offset) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
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
                    throw new IllegalStateException("Cache (" + nameWithPrefix + ") is closed !");
                } else if (isDestroyed()) {
                    throw new IllegalStateException("Cache (" + nameWithPrefix + ") is destroyed !");
                }

            }
        } catch (InterruptedException e) {
            ExceptionUtil.sneakyThrow(e);
        }
    }

    private <T extends EventListener> T initializeListener(ListenerConfig listenerConfig) {
        T listener = null;
        if (listenerConfig.getImplementation() != null) {
            listener = (T) listenerConfig.getImplementation();
        } else if (listenerConfig.getClassName() != null) {
            try {
                return ClassLoaderUtil.newInstance(getNodeEngine().getConfigClassLoader(),
                        listenerConfig.getClassName());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        return listener;
    }

    @Override
    public CacheStatistics getLocalCacheStatistics() {
        // TODO Throw `UnsupportedOperationException` if cache statistics are not enabled
        // but it breaks backward compatibility.
        final ICacheService service = getService();
        return service.createCacheStatIfAbsent(cacheConfig.getNameWithPrefix());
    }

}
