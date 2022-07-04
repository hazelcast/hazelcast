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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.impl.event.CachePartitionLostEventFilter;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.cache.impl.event.InternalCachePartitionLostListenerAdapter;
import com.hazelcast.cache.impl.operation.CacheListenerRegistrationOperation;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.internal.util.executor.CompletableFutureTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.internal.partition.IPartitionService;

import javax.annotation.Nonnull;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EventListener;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.ExceptionUtil.rethrowAllowedTypeFirst;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import com.hazelcast.spi.tenantcontrol.DestroyEventContext;

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
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
abstract class CacheProxySupport<K, V>
        extends AbstractDistributedObject<ICacheService>
        implements ICacheInternal<K, V>, CacheSyncListenerCompleter {

    private static final int TIMEOUT = 10;

    protected final ILogger logger;
    protected CacheConfig<K, V> cacheConfig;
    protected final String name;
    protected final String nameWithPrefix;
    protected final ICacheService cacheService;
    protected final SerializationService serializationService;
    protected final CacheOperationProvider operationProvider;
    protected final IPartitionService partitionService;

    private final CopyOnWriteArrayList<Future> loadAllTasks = new CopyOnWriteArrayList<>();

    private final AtomicReference<HazelcastServerCacheManager> cacheManagerRef = new AtomicReference<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

    private final CacheProxySyncListenerCompleter listenerCompleter = new CacheProxySyncListenerCompleter(this);

    CacheProxySupport(CacheConfig<K, V> cacheConfig, NodeEngine nodeEngine, ICacheService cacheService) {
        super(nodeEngine, cacheService);
        this.name = cacheConfig.getName();
        this.nameWithPrefix = cacheConfig.getNameWithPrefix();
        this.cacheConfig = cacheConfig;
        this.logger = nodeEngine.getLogger(getClass());
        this.partitionService = nodeEngine.getPartitionService();
        this.cacheService = cacheService;
        this.serializationService = nodeEngine.getSerializationService();
        this.operationProvider =
                cacheService.getCacheOperationProvider(nameWithPrefix, cacheConfig.getInMemoryFormat());

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
    public String getName() {
        return name;
    }

    @Override
    protected String getDistributedObjectName() {
        return nameWithPrefix;
    }

    @Override
    public String getPrefixedName() {
        return nameWithPrefix;
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public void open() {
        if (isDestroyed.get()) {
            throw new IllegalStateException("Cache is already destroyed! Cannot be reopened");
        }
        isClosed.compareAndSet(true, false);
    }

    @Override
    public void close() {
        close0(false);
    }

    @Override
    public @Nonnull DestroyEventContext getDestroyContextForTenant() {
        return () -> cacheConfig = ((CacheService) cacheService).reSerializeCacheConfig(cacheConfig);
    }

    @Override
    protected boolean preDestroy() {
        close0(true);
        if (!isDestroyed.compareAndSet(false, true)) {
            return false;
        }
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CacheProxySupport that = (CacheProxySupport) o;
        if (nameWithPrefix != null ? !nameWithPrefix.equals(that.nameWithPrefix) : that.nameWithPrefix != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return nameWithPrefix != null ? nameWithPrefix.hashCode() : 0;
    }

    @Override
    public String toString() {
        return getClass().getName() + '{' + "name=" + name + ", nameWithPrefix=" + nameWithPrefix + '}';
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManagerRef.get();
    }

    @Override
    public void setCacheManager(HazelcastCacheManager cacheManager) {
        assert cacheManager instanceof HazelcastServerCacheManager;

        // optimistically assume the CacheManager is already set
        if (cacheManagerRef.get() == cacheManager) {
            return;
        }

        if (!this.cacheManagerRef.compareAndSet(null, (HazelcastServerCacheManager) cacheManager)) {
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
    public void countDownCompletionLatch(int countDownLatchId) {
        listenerCompleter.countDownCompletionLatch(countDownLatchId);
    }

    protected void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    @SuppressWarnings("unchecked")
    protected void createAndSubmitLoadAllTask(Set<Data> keysData, boolean replaceExistingValues,
                                              CompletionListener completionListener) {
        try {
            CacheProxyLoadAllTask loadAllTask = new CacheProxyLoadAllTask(getNodeEngine(), operationProvider, keysData,
                    replaceExistingValues, completionListener, getServiceName());
            ExecutionService executionService = getNodeEngine().getExecutionService();
            final CompletableFutureTask<Object> future = (CompletableFutureTask<Object>) executionService
                    .submit("loadAll-" + nameWithPrefix, loadAllTask);
            loadAllTasks.add(future);
            future.whenCompleteAsync((response, t) -> {
                loadAllTasks.remove(future);
                if (t != null) {
                    logger.warning("Problem in loadAll task", t);
                }
            });
        } catch (Exception e) {
            if (completionListener != null) {
                completionListener.onException(e);
            }
            throw new CacheException(e);
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T injectDependencies(Object obj) {
        ManagedContext managedContext = serializationService.getManagedContext();
        return (T) managedContext.initialize(obj);
    }

    protected <T> InvocationFuture<T> invoke(Operation op, Data keyData, boolean completionOperation) {
        int partitionId = getPartitionId(keyData);
        return invoke(op, partitionId, completionOperation);
    }

    protected <T> InvocationFuture<T> removeAsyncInternal(K key, V oldValue, boolean hasOldValue,
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

    protected  <T> InvocationFuture<T> replaceAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
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

    protected <T> InvocationFuture<T> putAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy,
                                                      boolean isGet, boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(value);
        Operation op = operationProvider.createPutOperation(keyData, valueData, expiryPolicy, isGet, IGNORE_COMPLETION);
        return invoke(op, keyData, withCompletionEvent);
    }

    protected InvocationFuture<Boolean> putIfAbsentAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy,
                                                                boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(value);
        Operation operation = operationProvider.createPutIfAbsentOperation(keyData, valueData, expiryPolicy, IGNORE_COMPLETION);
        return invoke(operation, keyData, withCompletionEvent);
    }

    protected void clearInternal() {
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

    protected void removeAllInternal(Set<? extends K> keys) {
        Set<Data> keysData = null;
        if (keys != null) {
            keysData = createHashSet(keys.size());
            for (K key : keys) {
                validateNotNull(key);
                keysData.add(serializationService.toData(key));
            }
        }
        int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();
        Integer completionId = listenerCompleter.registerCompletionLatch(partitionCount);
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
            listenerCompleter.waitCompletionLatch(completionId, partitionCount - completionCount);
        } catch (Throwable t) {
            listenerCompleter.deregisterCompletionLatch(completionId);
            throw rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    protected void addListenerLocally(UUID regId, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        listenerCompleter.putListenerIfAbsent(cacheEntryListenerConfiguration, regId);
    }

    protected void removeListenerLocally(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        listenerCompleter.removeListener(cacheEntryListenerConfiguration);
    }

    protected UUID getListenerIdLocal(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        return listenerCompleter.getListenerId(cacheEntryListenerConfiguration);
    }

    protected  <T> T invokeInternal(Data keyData, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
            throws EntryProcessorException {
        Integer completionId = listenerCompleter.registerCompletionLatch(1);
        Operation op = operationProvider.createEntryProcessorOperation(keyData, completionId, entryProcessor, arguments);
        try {
            OperationService operationService = getNodeEngine().getOperationService();
            int partitionId = getPartitionId(keyData);
            InvocationFuture<T> future = operationService.invokeOnPartition(getServiceName(), op, partitionId);
            T safely = future.joinInternal();
            listenerCompleter.waitCompletionLatch(completionId);
            return safely;
        } catch (CacheException ce) {
            listenerCompleter.deregisterCompletionLatch(completionId);
            throw ce;
        } catch (Exception e) {
            listenerCompleter.deregisterCompletionLatch(completionId);
            throw new EntryProcessorException(e);
        }
    }

    protected void updateCacheListenerConfigOnOtherNodes(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration,
                                                       boolean isRegister) {
        OperationService operationService = getNodeEngine().getOperationService();
        Collection<Member> members = getNodeEngine().getClusterService().getMembers();
        for (Member member : members) {
            if (!member.localMember()) {
                Operation op = new CacheListenerRegistrationOperation(getDistributedObjectName(), cacheEntryListenerConfiguration,
                        isRegister);
                operationService.invokeOnTarget(CacheService.SERVICE_NAME, op, member.getAddress());
            }
        }
    }

    protected List<Data>[] groupDataToPartitions(Collection<? extends K> keys, int partitionCount) {
        List<Data>[] keysPerPartition = new ArrayList[partitionCount];

        for (K key: keys) {
            validateNotNull(key);

            Data dataKey = serializationService.toData(key);

            int partitionId = partitionService.getPartitionId(dataKey);

            List<Data> partition = keysPerPartition[partitionId];
            if (partition == null) {
                partition = new ArrayList<>();
                keysPerPartition[partitionId] = partition;
            }
            partition.add(dataKey);
        }

        return keysPerPartition;
    }

    protected void putToAllPartitionsAndWaitForCompletion(List<Map.Entry<Data, Data>>[] entriesPerPartition,
                                                        ExpiryPolicy expiryPolicy) throws Exception {
        List<Future> futures = new ArrayList<>(entriesPerPartition.length);
        for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
            List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
            if (entries != null) {
                // TODO: if there is a single entry, we could make use of a put operation since that is a bit cheaper
                Operation operation = operationProvider.createPutAllOperation(entries, expiryPolicy, partitionId);
                Future future = invoke(operation, partitionId, true);
                futures.add(future);
            }
        }

        Throwable error = null;
        for (Future future : futures) {
            try {
                future.get();
            } catch (Throwable t) {
                logger.finest("Error occurred while putting entries as batch!", t);
                if (error == null) {
                    error = t;
                }
            }
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

    protected void setTTLAllPartitionsAndWaitForCompletion(List<Data>[] keysPerPartition, Data expiryPolicy) {
        List<Future> futures = new ArrayList<>(keysPerPartition.length);
        for (int partitionId = 0; partitionId < keysPerPartition.length; partitionId++) {
            List<Data> keys = keysPerPartition[partitionId];
            if (keys != null) {
                Operation operation = operationProvider.createSetExpiryPolicyOperation(keys, expiryPolicy);
                futures.add(invoke(operation, partitionId, true));
            }
        }

        List<Throwable> throwables = FutureUtil.waitUntilAllResponded(futures);

        if (throwables.size() > 0) {
            throw rethrow(throwables.get(0));
        }
    }

    protected PartitionIdSet getPartitionsForKeys(Set<Data> keys) {
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        int partitions = partitionService.getPartitionCount();
        PartitionIdSet partitionIds = new PartitionIdSet(partitions);

        Iterator<Data> iterator = keys.iterator();
        int addedPartitions = 0;
        while (iterator.hasNext() && addedPartitions < partitions) {
            Data key = iterator.next();
            if (partitionIds.add(partitionService.getPartitionId(key))) {
                addedPartitions++;
            }
        }
        return partitionIds;
    }

    private void deregisterAllCacheEntryListener(Collection<UUID> listenerRegistrations) {
        ICacheService service = getService();
        for (UUID regId : listenerRegistrations) {
            service.deregisterListener(nameWithPrefix, regId);
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
        listener = injectDependencies(listener);
        return listener;
    }

    private void close0(boolean destroy) {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        Exception caughtException = null;
        for (Future f : loadAllTasks) {
            try {
                f.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (caughtException == null) {
                    caughtException = e;
                }
                getNodeEngine().getLogger(getClass()).warning("Problem while waiting for loadAll tasks to complete", e);
            }
        }
        loadAllTasks.clear();

        closeListeners();
        if (!destroy) {
            // when cache is being destroyed, the CacheManager is still required for cleanup and reset in postDestroy
            // when cache is being closed, the CacheManager is reset now
            resetCacheManager();
        }
        if (caughtException != null) {
            throw new CacheException("Problem while waiting for loadAll tasks to complete", caughtException);
        }
    }

    private void closeListeners() {
        deregisterAllCacheEntryListener(listenerCompleter.getListenersIds(true));
        deregisterAllCacheEntryListener(listenerCompleter.getListenersIds(false));

        listenerCompleter.clearListeners();
    }

    private <T> InvocationFuture<T> invoke(Operation op, int partitionId, boolean completionOperation) {
        Integer completionId = null;
        if (completionOperation) {
            completionId = listenerCompleter.registerCompletionLatch(1);
            if (op instanceof MutableOperation) {
                ((MutableOperation) op).setCompletionId(completionId);
            }
        }
        try {
            InvocationFuture<T> future = getNodeEngine().getOperationService()
                                                        .invokeOnPartition(getServiceName(), op, partitionId);
            if (completionOperation) {
                listenerCompleter.waitCompletionLatch(completionId);
            }
            return future;
        } catch (Throwable e) {
            if (e instanceof IllegalStateException) {
                close();
            }
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        } finally {
            if (completionOperation) {
                listenerCompleter.deregisterCompletionLatch(completionId);
            }
        }
    }
}
