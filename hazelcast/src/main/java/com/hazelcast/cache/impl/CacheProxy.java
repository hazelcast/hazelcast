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

import com.hazelcast.cache.impl.operation.CacheClearOperationFactory;
import com.hazelcast.cache.impl.operation.CacheContainsKeyOperation;
import com.hazelcast.cache.impl.operation.CacheEntryProcessorOperation;
import com.hazelcast.cache.impl.operation.CacheListenerRegistrationOperation;
import com.hazelcast.cache.impl.operation.CacheLoadAllOperationFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.cache.impl.CacheProxyUtil.getPartitionId;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;

/**
 * ICache implementation
 *
 * @param <K> key
 * @param <V> value
 */
public class CacheProxy<K, V>
        extends AbstractCacheProxy<K, V> {

    private static final int AWAIT_COMPLETION_TIMEOUT_SECONDS = 30;
    protected final ILogger logger;

    private HazelcastCacheManager cacheManager;

    protected CacheProxy(CacheConfig cacheConfig, CacheDistributedObject delegate, HazelcastServerCacheManager cacheManager) {
        super(cacheConfig, delegate);
        this.cacheManager = cacheManager;
        logger = nodeEngine.getLogger(getClass());
    }

    //region javax.cache.Cache<K, V> IMPL

    @Override
    public V get(K key) {
        return get(key, null);
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        return getAll(keys, null);
    }

    @Override
    public boolean containsKey(K key) {
        ensureOpen();
        validateNotNull(key);
        final Data k = serializationService.toData(key);
        final Operation op = new CacheContainsKeyOperation(getDistributedObjectName(), k);
        final InternalCompletableFuture<Boolean> f = nodeEngine.getOperationService().invokeOnPartition(getServiceName(), op,
                getPartitionId(nodeEngine, k));
        return f.getSafely();
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        ensureOpen();
        validateNotNull(keys);
        validateConfiguredTypes(keys);
        validateCacheLoader(completionListener);
        HashSet<Data> keysData = new HashSet<Data>();
        for (K key : keys) {
            keysData.add(serializationService.toData(key));
        }
        final OperationFactory operationFactory = new CacheLoadAllOperationFactory(getDistributedObjectName(), keysData,
                replaceExistingValues);
        try {
            submitLoadAllTask(operationFactory, completionListener);
        } catch (Exception e) {
            if (completionListener != null) {
                completionListener.onException(e);
            }
            throw new CacheException(e);
        }
    }

    @Override
    public void put(K key, V value) {
        put(key, value, null);
    }

    @Override
    public V getAndPut(K key, V value) {
        return getAndPut(key, value, null);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        putAll(map, null);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, null);
    }

    @Override
    public boolean remove(K key) {
        final InternalCompletableFuture<Boolean> f = removeAsyncInternal(key, null, false, false, true);
        try {
            return f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean remove(K key, V oldValue) {
        final InternalCompletableFuture<Boolean> f = removeAsyncInternal(key, oldValue, true, false, true);
        try {
            return f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public V getAndRemove(K key) {
        final InternalCompletableFuture<V> f = removeAsyncInternal(key, null, false, true, true);
        try {
            return f.get();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return replace(key, oldValue, newValue, null);
    }

    @Override
    public boolean replace(K key, V value) {
        final ExpiryPolicy expiryPolicy = null;
        return replace(key, value, expiryPolicy);
    }

    @Override
    public V getAndReplace(K key, V value) {
        return getAndReplace(key, value, null);
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        ensureOpen();
        validateNotNull(keys);
        removeAllInternal(keys, true);
    }

    @Override
    public void removeAll() {
        ensureOpen();
        removeAllInternal(null, true);
    }

    @Override
    public void clear() {
        ensureOpen();
        removeAllInternal(null, false);
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(cacheConfig)) {
            return clazz.cast(cacheConfig);
        }
        throw new IllegalArgumentException("The configuration class " + clazz + " is not supported by this implementation");
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
            throws EntryProcessorException {
        ensureOpen();
        validateNotNull(key);
        if (entryProcessor == null) {
            throw new NullPointerException("Entry Processor is null");
        }
        final Data k = serializationService.toData(key);
        final Integer completionId = registerCompletionLatch(1);
        final Operation op = new CacheEntryProcessorOperation(getDistributedObjectName(), k, completionId, entryProcessor,
                arguments);
        try {
            final InternalCompletableFuture<T> f = nodeEngine.getOperationService().invokeOnPartition(getServiceName(), op,
                    getPartitionId(nodeEngine, k));
            final T safely = f.getSafely();
            waitCompletionLatch(completionId);
            return safely;
        } catch (CacheException ce) {
            deregisterCompletionLatch(completionId);
            throw ce;
        } catch (Exception e) {
            deregisterCompletionLatch(completionId);
            throw new EntryProcessorException(e);
        }
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
                                                         Object... arguments) {
        //TODO implement a Multiple invoke operation and its factory
        ensureOpen();
        validateNotNull(keys);
        if (entryProcessor == null) {
            throw new NullPointerException("Entry Processor is null");
        }
        Map<K, EntryProcessorResult<T>> allResult = new HashMap<K, EntryProcessorResult<T>>();
        for (K key : keys) {
            CacheEntryProcessorResult<T> ceResult;
            try {
                final T result = this.invoke(key, entryProcessor, arguments);
                ceResult = result != null ? new CacheEntryProcessorResult<T>(result) : null;
            } catch (Exception e) {
                ceResult = new CacheEntryProcessorResult<T>(e);
            }
            if (ceResult != null) {
                allResult.put(key, ceResult);
            }
        }
        return allResult;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(((Object) this).getClass())) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("Unwrapping to " + clazz + " is not supported by this implementation");
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        ensureOpen();
        if (cacheEntryListenerConfiguration == null) {
            throw new NullPointerException("CacheEntryListenerConfiguration can't be " + "null");
        }
        final CacheService service = getService();
        final CacheEventListenerAdaptor<K, V> entryListener = new CacheEventListenerAdaptor<K, V>(this,
                cacheEntryListenerConfiguration, nodeEngine.getSerializationService());
        final String regId = service.registerListener(getDistributedObjectName(), entryListener);
        if (regId != null) {
            cacheConfig.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
            addListenerLocally(regId, cacheEntryListenerConfiguration);
            //CREATE ON OTHERS TOO
            registrationOtherNodes(cacheEntryListenerConfiguration, true);
        }
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        if (cacheEntryListenerConfiguration == null) {
            throw new NullPointerException("CacheEntryListenerConfiguration can't be " + "null");
        }
        final CacheService service = getService();
        final String regId = removeListenerLocally(cacheEntryListenerConfiguration);
        if (regId != null) {
            if (!service.deregisterListener(getDistributedObjectName(), regId)) {
                addListenerLocally(regId, cacheEntryListenerConfiguration);
            } else {
                cacheConfig.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
                deregisterCompletionListener();
                //REMOVE ON OTHERS TOO
                registrationOtherNodes(cacheEntryListenerConfiguration, false);
            }
        }
    }

    protected void registrationOtherNodes(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration,
                                          boolean isRegister) {
        final OperationService operationService = nodeEngine.getOperationService();
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        Collection<Future> futures = new ArrayList<Future>();
        for (MemberImpl member : members) {
            if (!member.localMember()) {
                final Operation op = new CacheListenerRegistrationOperation(getDistributedObjectName(),
                        cacheEntryListenerConfiguration, isRegister);
                final InternalCompletableFuture<Object> future = operationService
                        .invokeOnTarget(CacheService.SERVICE_NAME, op, member.getAddress());
                futures.add(future);
            }
        }
        //make sure all configs are created
        try {
            FutureUtil.waitWithDeadline(futures, AWAIT_COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            logger.warning(e);
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        ensureOpen();
        return new ClusterWideIterator<K, V>(this, delegate);
    }
    //endregion

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
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        final Integer completionId = registerCompletionLatch(partitionCount);
        final OperationService operationService = nodeEngine.getOperationService();
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

}
