/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.operation.AbstractMutatingCacheOperation;
import com.hazelcast.cache.impl.operation.CacheGetAndRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheGetAndReplaceOperation;
import com.hazelcast.cache.impl.operation.CachePutIfAbsentOperation;
import com.hazelcast.cache.impl.operation.CachePutOperation;
import com.hazelcast.cache.impl.operation.CacheRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheReplaceOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.CompletableFutureTask;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CompletionListener;
import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.impl.CacheProxyUtil.getPartitionId;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateResults;

/**
 * Base Cache Proxy
 */
abstract class AbstractBaseCacheProxy<K, V> {

    static final int TIMEOUT = 10;

    protected final CacheDistributedObject delegate;
    protected final CacheConfig<K, V> cacheConfig;
    //this will represent the name from the user perspective
    protected final String name;
    protected final NodeEngine nodeEngine;
    protected final SerializationService serializationService;

    private final CopyOnWriteArrayList<Future> loadAllTasks = new CopyOnWriteArrayList<Future>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations;
    private final ConcurrentMap<Integer, CountDownLatch> syncLocks;

    private final AtomicInteger completionIdCounter = new AtomicInteger();
    private final CacheLoader<K, V> cacheLoader;
    private final Object completionRegistrationMutex = new Object();
    private volatile String completionRegistrationId;

    protected AbstractBaseCacheProxy(CacheConfig cacheConfig, CacheDistributedObject delegate) {
        this.name = cacheConfig.getName();
        this.cacheConfig = cacheConfig;
        this.delegate = delegate;
        this.nodeEngine = delegate.getNodeEngine();
        this.serializationService = this.nodeEngine.getSerializationService();
        if (cacheConfig.getCacheLoaderFactory() != null) {
            final Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
            cacheLoader = cacheLoaderFactory.create();
        } else {
            cacheLoader = null;
        }
        asyncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncLocks = new ConcurrentHashMap<Integer, CountDownLatch>();

    }

    protected void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    protected <T> InternalCompletableFuture<T> invoke(Operation op, Data keyData, boolean completionOperation) {
        final int partitionId = getPartitionId(nodeEngine, keyData);
        Integer completionId = null;
        if (completionOperation) {
            completionId = registerCompletionLatch(1);
            if (op instanceof AbstractMutatingCacheOperation) {
                ((AbstractMutatingCacheOperation) op).setCompletionId(completionId);
            }
        }
        try {
            final InternalCompletableFuture<T> f = nodeEngine.getOperationService()
                                                             .invokeOnPartition(getServiceName(), op, partitionId);
            if (completionOperation) {
                waitCompletionLatch(completionId);
            }
            return f;
        } catch (Throwable e) {
            if (e instanceof IllegalStateException) {
                isClosed.set(true);
            }
            if (completionOperation) {
                deregisterCompletionLatch(completionId);
            }
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }

    }

    protected <T> InternalCompletableFuture<T> removeAsyncInternal(K key, V oldValue, boolean hasOldValue, boolean isGet,
                                                                   boolean withCompletionEvent) {
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue);
            validateConfiguredTypes(true, key, oldValue);
        } else {
            validateNotNull(key);
            validateConfiguredTypes(false, key);
        }
        final Data keyData = serializationService.toData(key);
        final Data valueData = oldValue != null ? serializationService.toData(oldValue) : null;
        final Operation operation;
        if (isGet) {
            operation = new CacheGetAndRemoveOperation(getDistributedObjectName(), keyData);
        } else {
            operation = new CacheRemoveOperation(getDistributedObjectName(), keyData, valueData);
        }
        return invoke(operation, keyData, withCompletionEvent);
    }

    protected <T> InternalCompletableFuture<T> replaceAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                                    boolean hasOldValue, boolean isGet,
                                                                    boolean withCompletionEvent) {
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            validateConfiguredTypes(true, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            validateConfiguredTypes(true, key, newValue);
        }
        final Data keyData = serializationService.toData(key);
        final Data oldValueData = oldValue != null ? serializationService.toData(oldValue) : null;
        final Data newValueData = serializationService.toData(newValue);
        final Operation operation;
        if (isGet) {
            operation = new CacheGetAndReplaceOperation(getDistributedObjectName(), keyData, newValueData, expiryPolicy);
        } else {
            operation = new CacheReplaceOperation(getDistributedObjectName(), keyData, oldValueData, newValueData, expiryPolicy);
        }
        return invoke(operation, keyData, withCompletionEvent);
    }

    protected <T> InternalCompletableFuture<T> putAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy, boolean isGet,
                                                                boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        validateConfiguredTypes(true, key, value);
        final Data keyData = serializationService.toData(key);
        final Data valueData = serializationService.toData(value);
        final Operation op = new CachePutOperation(getDistributedObjectName(), keyData, valueData, expiryPolicy, isGet);
        return invoke(op, keyData, withCompletionEvent);
    }

    protected InternalCompletableFuture<Boolean> putIfAbsentAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy,
                                                                          boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        validateConfiguredTypes(true, key, value);
        final Data keyData = serializationService.toData(key);
        final Data valueData = serializationService.toData(value);
        final Operation op = new CachePutIfAbsentOperation(getDistributedObjectName(), keyData, valueData, expiryPolicy);
        return invoke(op, keyData, withCompletionEvent);
    }

    protected void validateConfiguredTypes(boolean validateValues, K key, V... values)
            throws ClassCastException {
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, validateValues, key, values);
    }

    protected void validateConfiguredTypes(Set<? extends K> keys)
            throws ClassCastException {
        for (K key : keys) {
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, false, key);
        }
    }

    protected void submitLoadAllTask(final OperationFactory operationFactory, final CompletionListener completionListener) {
        final LoadAllTask loadAllTask = new LoadAllTask(operationFactory, completionListener);
        final ExecutionService executionService = nodeEngine.getExecutionService();
        final CompletableFutureTask<?> future = (CompletableFutureTask<?>) executionService
                .submit("loadAll-" + delegate.getName(), loadAllTask);
        loadAllTasks.add(future);
        future.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                loadAllTasks.remove(future);
            }

            @Override
            public void onFailure(Throwable t) {
                loadAllTasks.remove(future);
            }
        });
    }

    protected void addListenerLocally(String regId, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        if (cacheEntryListenerConfiguration.isSynchronous()) {
            syncListenerRegistrations.putIfAbsent(cacheEntryListenerConfiguration, regId);
            registerCompletionListener();
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

    protected void validateCacheLoader(CompletionListener completionListener) {
        if (cacheLoader == null && completionListener != null) {
            completionListener.onCompletion();
        }
    }

    protected abstract boolean isDefaultClassLoader();

    //region base cache method impls

    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        for (Future f : loadAllTasks) {
            try {
                f.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new CacheException(e);
            }
        }
        loadAllTasks.clear();
        //TODO close cache issue:
        //        if(!isDefaultClassLoader()){
        delegate.destroy();
        //        }
        //close the configured CacheLoader
        closeCacheLoader();
        deregisterCompletionListener();
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    //endregion

    //region DISTRIBUTED OBJECT
    protected String getDistributedObjectName() {
        return delegate.getName();
    }

    protected String getServiceName() {
        return delegate.getServiceName();
    }

    protected CacheService getService() {
        return delegate.getService();
    }

    protected NodeEngine getNodeEngine() {
        return delegate.getNodeEngine();
    }

    protected void closeCacheLoader() {
        //close the configured CacheLoader
        if (cacheLoader instanceof Closeable) {
            IOUtil.closeResource((Closeable) cacheLoader);
        }
    }
    //endregion

    protected void countDownCompletionLatch(int id) {
        final CountDownLatch countDownLatch = syncLocks.get(id);
        countDownLatch.countDown();
        if (countDownLatch.getCount() == 0) {
            deregisterCompletionLatch(id);
        }
    }

    protected Integer registerCompletionLatch(int count) {
        if (!syncListenerRegistrations.isEmpty()) {
            final int id = completionIdCounter.incrementAndGet();
            CountDownLatch countDownLatch = new CountDownLatch(count);
            syncLocks.put(id, countDownLatch);
            return id;
        }
        return AbstractMutatingCacheOperation.IGNORE_COMPLETION;
    }

    protected void deregisterCompletionLatch(Integer countDownLatchId) {
        syncLocks.remove(countDownLatchId);
    }

    protected void waitCompletionLatch(Integer countDownLatchId) {
        final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
        if (countDownLatch != null) {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    protected void waitCompletionLatch(Integer countDownLatchId, int offset) {
        //fix completion count
        final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
        if (countDownLatch != null) {
            for (int i = 0; i < offset; i++) {
                countDownLatch.countDown();
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    protected void registerCompletionListener() {
        if (!syncListenerRegistrations.isEmpty() && completionRegistrationId == null) {
            synchronized (completionRegistrationMutex) {
                if (completionRegistrationId == null) {
                    final CacheService service = getService();
                    CacheEventListener entryListener = new CacheCompletionEventListener();
                    completionRegistrationId = service.registerListener(getDistributedObjectName(), entryListener);
                }
            }
        }
    }

    protected void deregisterCompletionListener() {
        if (syncListenerRegistrations.isEmpty() && completionRegistrationId != null) {
            synchronized (completionRegistrationMutex) {
                if (completionRegistrationId != null) {
                    final CacheService service = getService();
                    final boolean isDeregistered = service
                            .deregisterListener(getDistributedObjectName(), completionRegistrationId);
                    if (isDeregistered) {
                        completionRegistrationId = null;
                    }
                }
            }
        }
    }

    private final class CacheCompletionEventListener
            implements CacheEventListener {

        @Override
        public void handleEvent(Object eventObject) {
            if (eventObject instanceof CacheEventData) {
                CacheEventData cacheEventData = (CacheEventData) eventObject;
                if (cacheEventData.getCacheEventType() == CacheEventType.COMPLETED) {
                    Integer completionId = serializationService.toObject(cacheEventData.getDataValue());
                    countDownCompletionLatch(completionId);
                }
            }
        }
    }

    private final class LoadAllTask
            implements Runnable {

        private final OperationFactory operationFactory;
        private final CompletionListener completionListener;

        private LoadAllTask(OperationFactory operationFactory, CompletionListener completionListener) {
            this.operationFactory = operationFactory;
            this.completionListener = completionListener;
        }

        @Override
        public void run() {
            try {
                final OperationService operationService = nodeEngine.getOperationService();
                final Map<Integer, Object> results = operationService.invokeOnAllPartitions(getServiceName(), operationFactory);
                validateResults(results);
                if (completionListener != null) {
                    completionListener.onCompletion();
                }
            } catch (Exception e) {
                if (completionListener != null) {
                    completionListener.onException(e);
                }
            }
        }
    }

}
