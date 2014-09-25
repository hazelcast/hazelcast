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

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.operation.AbstractMutatingCacheOperation;
import com.hazelcast.cache.impl.operation.CacheClearOperationFactory;
import com.hazelcast.cache.impl.operation.CacheGetAndRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheGetAndReplaceOperation;
import com.hazelcast.cache.impl.operation.CachePutIfAbsentOperation;
import com.hazelcast.cache.impl.operation.CachePutOperation;
import com.hazelcast.cache.impl.operation.CacheRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheReplaceOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.impl.CacheProxyUtil.getPartitionId;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;

/**
 * Base Cache Proxy
 */
abstract class AbstractCacheProxyInternal<K, V>
        extends AbstractCacheProxyBase<K, V>
        implements ICache<K, V> {

    private final ConcurrentMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations;

    private final ConcurrentMap<Integer, CountDownLatch> syncLocks;
    private final AtomicInteger completionIdCounter = new AtomicInteger();
    private final Object completionRegistrationMutex = new Object();
    private volatile String completionRegistrationId;

    protected AbstractCacheProxyInternal(CacheConfig cacheConfig, NodeEngine nodeEngine, CacheService cacheService) {
        super(cacheConfig, nodeEngine, cacheService);
        asyncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncLocks = new ConcurrentHashMap<Integer, CountDownLatch>();
    }

    protected <T> InternalCompletableFuture<T> invoke(Operation op, Data keyData, boolean completionOperation) {
        Integer completionId = null;
        if (completionOperation) {
            completionId = registerCompletionLatch(1);
            if (op instanceof AbstractMutatingCacheOperation) {
                ((AbstractMutatingCacheOperation) op).setCompletionId(completionId);
            }
        }
        try {
            final int partitionId = getPartitionId(getNodeEngine(), keyData);
            final InternalCompletableFuture<T> f = getNodeEngine().getOperationService()
                                                                  .invokeOnPartition(getServiceName(), op, partitionId);
            if (completionOperation) {
                waitCompletionLatch(completionId);
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

    //region internal base operations
    protected <T> InternalCompletableFuture<T> removeAsyncInternal(K key, V oldValue, boolean hasOldValue, boolean isGet,
                                                                   boolean withCompletionEvent) {
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue);
        } else {
            validateNotNull(key);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key);
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
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, newValue);
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
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        final Data keyData = serializationService.toData(key);
        final Data valueData = serializationService.toData(value);
        final Operation op = new CachePutOperation(getDistributedObjectName(), keyData, valueData, expiryPolicy, isGet);
        return invoke(op, keyData, withCompletionEvent);
    }

    protected InternalCompletableFuture<Boolean> putIfAbsentAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy,
                                                                          boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        final Data keyData = serializationService.toData(key);
        final Data valueData = serializationService.toData(value);
        final Operation op = new CachePutIfAbsentOperation(getDistributedObjectName(), keyData, valueData, expiryPolicy);
        return invoke(op, keyData, withCompletionEvent);
    }

    protected void removeAllInternal(Set<? extends K> keys, boolean isRemoveAll) {
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
        final CacheClearOperationFactory operationFactory = new CacheClearOperationFactory(getDistributedObjectName(), keysData,
                isRemoveAll, completionId);
        try {
            final Map<Integer, Object> results = operationService.invokeOnAllPartitions(getServiceName(), operationFactory);
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
    //endregion internal base operations

    //region Listener operations
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

    public void deregisterAllCacheEntryListener(Collection<String> listenerRegistrations) {
        final CacheService service = getService();
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

        deregisterCompletionListener();
    }

    protected void countDownCompletionLatch(int id) {
        final CountDownLatch countDownLatch = syncLocks.get(id);
        if (countDownLatch != null) {
            countDownLatch.countDown();
            if (countDownLatch.getCount() == 0) {
                deregisterCompletionLatch(id);
            }
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

    //endregion Listener operations
}
