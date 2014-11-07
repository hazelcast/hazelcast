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

import com.hazelcast.cache.impl.operation.CacheListenerRegistrationOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;

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

import static com.hazelcast.cache.impl.CacheProxyUtil.getPartitionId;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;

/**
 * <h1>ICache implementation</h1>
 * <p>
 * This proxy is the implementation of ICache and javax.cache.Cache which is returned by
 * HazelcastServerCacheManager. It represents a cache for server or embedded mode.
 * </p>
 * <p>
 * Each cache method actually is an operation which is sent to related partition(s) or node(s).
 * Operations are executed on partition's or node's executor pools and the results are delivered to the user.
 * </p>
 * <p>
 * In order to access a {@linkplain CacheProxy} by name, a cacheManager should be used. It's advised to use
 * {@link com.hazelcast.cache.ICache} instead.
 * </p>
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
public class CacheProxy<K, V>
        extends AbstractCacheProxy<K, V> {

    protected final ILogger logger;

    private final AbstractHazelcastCacheManager cacheManager;

    protected CacheProxy(CacheConfig cacheConfig, NodeEngine nodeEngine, ICacheService cacheService,
                         HazelcastServerCacheManager cacheManager) {
        super(cacheConfig, nodeEngine, cacheService);
        this.cacheManager = cacheManager;
        this.logger = getNodeEngine().getLogger(getClass());
    }

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
//        final Operation op = new CacheContainsKeyOperation(getDistributedObjectName(), k);
        Operation operation = operationProvider.createContainsKeyOperation(k);
        OperationService operationService = getNodeEngine().getOperationService();
        int partitionId = getPartitionId(getNodeEngine(), k);
        InternalCompletableFuture<Boolean> f = operationService.invokeOnPartition(getServiceName(), operation, partitionId);

        return f.getSafely();
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        ensureOpen();
        validateNotNull(keys);
        for (K key : keys) {
            CacheProxyUtil.validateConfiguredKeyType(cacheConfig, key);
        }
        validateCacheLoader(completionListener);
        HashSet<Data> keysData = new HashSet<Data>();
        for (K key : keys) {
            keysData.add(serializationService.toData(key));
        }
        OperationFactory operationFactory = operationProvider.createLoadAllOperationFactory(keysData, replaceExistingValues);
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
        removeAllInternal(keys);
    }

    @Override
    public void removeAll() {
        ensureOpen();
        removeAllInternal(null);
    }

    @Override
    public void clear() {
        ensureOpen();
        clearInternal();
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(cacheConfig)) {
            return clazz.cast(cacheConfig.getAsReadOnly());
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
        final Data keyData = serializationService.toData(key);
        final Integer completionId = registerCompletionLatch(1);
        Operation op = operationProvider.createEntryProcessorOperation(keyData, completionId, entryProcessor, arguments);
        try {
            OperationService operationService = getNodeEngine().getOperationService();
            int partitionId = getPartitionId(getNodeEngine(), keyData);
            final InternalCompletableFuture<T> f = operationService.invokeOnPartition(getServiceName(), op, partitionId);
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
        final ICacheService service = getService();
        final CacheEventListenerAdaptor<K, V> entryListener = new CacheEventListenerAdaptor<K, V>(this,
                cacheEntryListenerConfiguration, getNodeEngine().getSerializationService());
        final String regId = service.registerListener(getDistributedObjectName(), entryListener);
        if (regId != null) {
            cacheConfig.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
            addListenerLocally(regId, cacheEntryListenerConfiguration);
            //CREATE ON OTHERS TOO
            updateCacheListenerConfigOnOtherNodes(cacheEntryListenerConfiguration, true);
        }
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        if (cacheEntryListenerConfiguration == null) {
            throw new NullPointerException("CacheEntryListenerConfiguration can't be " + "null");
        }
        final ICacheService service = getService();
        final String regId = removeListenerLocally(cacheEntryListenerConfiguration);
        if (regId != null) {
            if (!service.deregisterListener(getDistributedObjectName(), regId)) {
                addListenerLocally(regId, cacheEntryListenerConfiguration);
            } else {
                cacheConfig.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
                deregisterCompletionListener();
                //REMOVE ON OTHERS TOO
                updateCacheListenerConfigOnOtherNodes(cacheEntryListenerConfiguration, false);
            }
        }
    }

    protected void updateCacheListenerConfigOnOtherNodes(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration,
                                                         boolean isRegister) {
        final OperationService operationService = getNodeEngine().getOperationService();
        final Collection<MemberImpl> members = getNodeEngine().getClusterService().getMemberList();
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
        //TODO do we need this ???s
//        try {
//            FutureUtil.waitWithDeadline(futures, CacheProxyUtil.AWAIT_COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
//        } catch (TimeoutException e) {
//            logger.warning(e);
//        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        ensureOpen();
        return new ClusterWideIterator<K, V>(this);
    }
}
