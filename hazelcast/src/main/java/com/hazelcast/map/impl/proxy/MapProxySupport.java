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

package com.hazelcast.map.impl.proxy;

import com.hazelcast.concurrent.lock.LockProxySupport;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapContextQuerySupport;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.map.impl.MapEventPublisher;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.NearCache;
import com.hazelcast.map.impl.NearCacheProvider;
import com.hazelcast.map.impl.QueryEventFilter;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.map.impl.operation.AddIndexOperation;
import com.hazelcast.map.impl.operation.AddInterceptorOperation;
import com.hazelcast.map.impl.operation.BasePutOperation;
import com.hazelcast.map.impl.operation.BaseRemoveOperation;
import com.hazelcast.map.impl.operation.ClearOperation;
import com.hazelcast.map.impl.operation.ContainsKeyOperation;
import com.hazelcast.map.impl.operation.ContainsValueOperationFactory;
import com.hazelcast.map.impl.operation.DeleteOperation;
import com.hazelcast.map.impl.operation.EntryOperation;
import com.hazelcast.map.impl.operation.EvictAllOperation;
import com.hazelcast.map.impl.operation.EvictOperation;
import com.hazelcast.map.impl.operation.GetEntryViewOperation;
import com.hazelcast.map.impl.operation.GetOperation;
import com.hazelcast.map.impl.operation.IsEmptyOperationFactory;
import com.hazelcast.map.impl.operation.KeyBasedMapOperation;
import com.hazelcast.map.impl.operation.LoadAllOperation;
import com.hazelcast.map.impl.operation.LoadMapOperation;
import com.hazelcast.map.impl.operation.MapFlushOperation;
import com.hazelcast.map.impl.operation.MapGetAllOperationFactory;
import com.hazelcast.map.impl.operation.MultipleEntryOperationFactory;
import com.hazelcast.map.impl.operation.PartitionCheckIfLoadedOperationFactory;
import com.hazelcast.map.impl.operation.PartitionWideEntryWithPredicateOperationFactory;
import com.hazelcast.map.impl.operation.PutAllOperation;
import com.hazelcast.map.impl.operation.PutIfAbsentOperation;
import com.hazelcast.map.impl.operation.PutOperation;
import com.hazelcast.map.impl.operation.PutTransientOperation;
import com.hazelcast.map.impl.operation.RemoveIfSameOperation;
import com.hazelcast.map.impl.operation.RemoveInterceptorOperation;
import com.hazelcast.map.impl.operation.RemoveOperation;
import com.hazelcast.map.impl.operation.ReplaceIfSameOperation;
import com.hazelcast.map.impl.operation.ReplaceOperation;
import com.hazelcast.map.impl.operation.SetOperation;
import com.hazelcast.map.impl.operation.SizeOperationFactory;
import com.hazelcast.map.impl.operation.TryPutOperation;
import com.hazelcast.map.impl.operation.TryRemoveOperation;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.BinaryOperationFactory;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterableUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.util.executor.CompletedFuture;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.IterableUtil.nullToEmpty;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

abstract class MapProxySupport extends AbstractDistributedObject<MapService> implements InitializingObject {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    protected static final String NULL_PREDICATE_IS_NOT_ALLOWED = "Predicate should not be null!";
    protected static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";

    private static final Object FAST_GET_MISS = new Object();

    protected final String name;
    protected final LocalMapStatsImpl localMapStats;
    protected final LockProxySupport lockSupport;
    protected final PartitioningStrategy partitionStrategy;
    private InternalPartitionService partitionService;

    protected MapProxySupport(String name, MapService service, NodeEngine nodeEngine) {
        super(nodeEngine, service);
        this.name = name;
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        partitionStrategy = mapServiceContext.getMapContainer(name).getPartitioningStrategy();
        localMapStats = mapServiceContext.getLocalMapStatsProvider().getLocalMapStatsImpl(name);
        this.partitionService = getNodeEngine().getPartitionService();
        lockSupport = new LockProxySupport(new DefaultObjectNamespace(MapService.SERVICE_NAME, name));
    }

    @Override
    public void initialize() {
        initializeListeners();
        initializeIndexes();
        initializeMapStoreLoad();
    }

    private void initializeMapStoreLoad() {
        MapStoreConfig mapStoreConfig = getMapConfig().getMapStoreConfig();
        if (mapStoreConfig != null && mapStoreConfig.isEnabled()) {
            MapStoreConfig.InitialLoadMode initialLoadMode = mapStoreConfig.getInitialLoadMode();
            if (MapStoreConfig.InitialLoadMode.EAGER.equals(initialLoadMode)) {
                waitUntilLoaded();
            }
        }
    }

    private void initializeIndexes() {
        for (MapIndexConfig index : getMapConfig().getMapIndexConfigs()) {
            if (index.getAttribute() != null) {
                addIndex(index.getAttribute(), index.isOrdered());
            }
        }
    }

    private void initializeListeners() {
        final MapConfig mapConfig = getMapConfig();

        for (EntryListenerConfig listenerConfig : mapConfig.getEntryListenerConfigs()) {
            final MapListener listener = initializeListener(listenerConfig);
            if (listener != null) {
                if (listenerConfig.isLocal()) {
                    addLocalEntryListenerInternal(listener);
                } else {
                    addEntryListenerInternal(listener, null, listenerConfig.isIncludeValue());
                }
            }
        }

        for (MapPartitionLostListenerConfig listenerConfig : mapConfig.getPartitionLostListenerConfigs()) {
            final MapPartitionLostListener listener = initializeListener(listenerConfig);
            if (listener != null) {
                addPartitionLostListenerInternal(listener);
            }
        }
    }

    private <T extends EventListener> T initializeListener(ListenerConfig listenerConfig) {
        T listener = null;
        if (listenerConfig.getImplementation() != null) {
            listener = (T) listenerConfig.getImplementation();
        } else if (listenerConfig.getClassName() != null) {
            try {
                return ClassLoaderUtil
                        .newInstance(getNodeEngine().getConfigClassLoader(), listenerConfig.getClassName());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        if (listener instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) listener).setHazelcastInstance(getNodeEngine().getHazelcastInstance());
        }

        return listener;
    }

    // Unused: waiting to be tested. Unifies sync and async code paths for get.
    private Object unifiedGetInternal(Data key) {
        try {
            return getAsyncInternal(key).get();
        } catch (ExecutionException e) {
            throw ExceptionUtil.rethrow(e.getCause());
        } catch (InterruptedException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }


    // this operation returns the object in data format except when
    // it is retrieved from near-cache and near-cache memory format is object.
    protected final Object getInternal(Data key) {
        final Object fastValue = fastGet(key);
        if (fastValue != FAST_GET_MISS) {
            return fastValue;
        }
        final GetOperation operation = new GetOperation(name, key);
        if (getMapConfig().isNearCacheEnabled() && (cachingLocalEntries() || keyIsRemotelyOwned(key))) {
            final NearCacheProvider nearCache = getNearCacheProvider();
            final long permit = nearCache.announcePutNearCache(name, key);
            final Object value;
            try {
                value = syncInvoke(key, operation);
            } catch (Throwable t) {
                nearCache.cancelAnnouncement(name, permit, key);
                throw ExceptionUtil.rethrow(t);
            }
            nearCache.putNearCacheAnnounced(name, permit, key, (Data) value);
            return value;
        } else {
            return syncInvoke(key, operation);
        }
    }

    protected final ICompletableFuture<Data> getAsyncInternal(final Data key) {
        final Object fastValue = fastGet(key);
        if (fastValue != FAST_GET_MISS) {
            return completedFuture(fastValue);
        }
        final GetOperation operation = new GetOperation(name, key);
        if (getMapConfig().isNearCacheEnabled() && (cachingLocalEntries() || keyIsRemotelyOwned(key))) {
            final NearCacheProvider nearCache = getNearCacheProvider();
            final long permit = nearCache.announcePutNearCache(name, key);
            final InternalCompletableFuture<Data> future = asyncInvoke(key, operation);
            future.andThen(new ExecutionCallback<Data>() {
                @Override public void onResponse(Data response) {
                    nearCache.putNearCacheAnnounced(name, permit, key, response);
                }
                @Override public void onFailure(Throwable t) {
                    nearCache.cancelAnnouncement(name, permit, key);
                }
            });
            return future;
        } else {
            return asyncInvoke(key, operation);
        }
    }

    private Object fastGet(Data key) {
        final MapConfig mapConfig = getMapConfig();
        final boolean nearCacheEnabled = mapConfig.isNearCacheEnabled();
        if (nearCacheEnabled) {
            final Object cached = getNearCacheProvider().getFromNearCache(name, key);
            if (cached != null) {
                return cached == NearCache.NULL_OBJECT ? null : cached;
            }
        }
        //TODO Racy direct access to RecordStore without involving the partition thread
        if (mapConfig.isReadBackupData()) {
            final Object fromBackup = readBackupDataOrNull(key);
            if (fromBackup != null) {
                return fromBackup;
            }
        }
        return FAST_GET_MISS;
    }

    private CompletedFuture<Data> completedFuture(Object result) {
        final NodeEngine nodeEngine = getNodeEngine();
        return new CompletedFuture<Data>(
                nodeEngine.getSerializationService(),
                result,
                nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR));
    }

    private boolean keyIsRemotelyOwned(Data key) {
        final NodeEngine nodeEngine = getService().getMapServiceContext().getNodeEngine();
        final InternalPartitionService ps = nodeEngine.getPartitionService();
        final int partitionId = ps.getPartitionId(key);
        return !ps.getPartitionOwner(partitionId).equals(nodeEngine.getClusterService().getThisAddress());
    }

    private boolean cachingLocalEntries() {
        return getMapConfig().getNearCacheConfig().isCacheLocalEntries();
    }

    private Object getFromNearCache(Data key) {
        final Object cached = getNearCacheProvider().getFromNearCache(name, key);
        if (cached == null) {
            return null;
        }
        getService().getMapServiceContext().interceptAfterGet(name, cached);
        return cached;
    }

    private void getFromNearCache(Map<Object, Object> resultMap, Collection<Data> keys) {
        if (!getMapConfig().isNearCacheEnabled()) {
            return;
        }
        final MapService mapService = getService();
        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            Data key = iterator.next();
            final Object cached = getFromNearCache(key);
            if (cached == null) {
                continue;
            }
            if (cached != NearCache.NULL_OBJECT) {
                final MapServiceContext ctx = mapService.getMapServiceContext();
                resultMap.put(ctx.toObject(key), ctx.toObject(cached));
            }
            iterator.remove();
        }
    }

    private Data readBackupDataOrNull(Data key) {
        final MapServiceContext mapServiceContext = getService().getMapServiceContext();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final int partitionId = partitionService.getPartitionId(key);
        if (!partitionService.getPartition(partitionId, false).isOwnerOrBackup(nodeEngine.getThisAddress())) {
            return null;
        }
        final RecordStore recordStore =
                mapServiceContext.getPartitionContainer(partitionId).getExistingRecordStore(name);
        return recordStore != null ? recordStore.readBackupData(key) : null;
    }

    protected Data putInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        PutOperation operation = new PutOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        Data previousValue = (Data) syncInvoke(key, operation);
        invalidateNearCache(key);
        return previousValue;
    }

    protected boolean tryPutInternal(final Data key, final Data value, final long timeout, final TimeUnit timeunit) {
        TryPutOperation operation = new TryPutOperation(name, key, value, getTimeInMillis(timeout, timeunit));
        boolean putSuccessful = (Boolean) syncInvoke(key, operation);
        invalidateNearCache(key);
        return putSuccessful;
    }

    protected Data putIfAbsentInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        PutIfAbsentOperation operation = new PutIfAbsentOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        Data previousValue = (Data) syncInvoke(key, operation);
        invalidateNearCache(key);
        return previousValue;
    }

    protected void putTransientInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        PutTransientOperation operation = new PutTransientOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        syncInvoke(key, operation);
        invalidateNearCache(key);
    }

    protected ICompletableFuture<Data> putAsyncInternal(final Data key, final Data value,
                                                        final long ttl, final TimeUnit timeunit) {
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        PutOperation operation = new PutOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            ICompletableFuture<Data> future
                    = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
            invalidateNearCache(key);
            return future;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected boolean replaceInternal(final Data key, final Data expect, final Data update) {
        ReplaceIfSameOperation operation = new ReplaceIfSameOperation(name, key, expect, update);
        boolean replaceSuccessful = (Boolean) syncInvoke(key, operation);
        invalidateNearCache(key);
        return replaceSuccessful;
    }

    protected Data replaceInternal(final Data key, final Data value) {
        ReplaceOperation operation = new ReplaceOperation(name, key, value);
        final Data result = (Data) syncInvoke(key, operation);
        invalidateNearCache(key);
        return result;
    }

    protected void setInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        SetOperation operation = new SetOperation(name, key, value, timeunit.toMillis(ttl));
        syncInvoke(key, operation);
        invalidateNearCache(key);
    }

    protected boolean evictInternal(final Data key) {
        EvictOperation operation = new EvictOperation(name, key, false);
        final boolean evictSuccess = (Boolean) syncInvoke(key, operation);
        invalidateNearCache(key);
        return evictSuccess;
    }

    protected void evictAllInternal() {
        try {
            clearNearCache();
            final Operation operation = new EvictAllOperation(name);
            final NodeEngine nodeEngine = getNodeEngine();
            final Map<Integer, Object> resultMap
                    = nodeEngine.getOperationService().invokeOnAllPartitions(SERVICE_NAME,
                    new BinaryOperationFactory(operation, nodeEngine));

            int numberOfAffectedEntries = 0;
            for (Object o : resultMap.values()) {
                numberOfAffectedEntries += (Integer) o;
            }
            if (numberOfAffectedEntries > 0) {
                publishMapEvent(numberOfAffectedEntries, EntryEventType.EVICT_ALL);
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected void loadAllInternal(boolean replaceExistingValues) {
        NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int mapNamePartition = partitionService.getPartitionId(name);

        Operation operation = new LoadMapOperation(name, replaceExistingValues);
        Future loadMapFuture = operationService.invokeOnPartition(MapService.SERVICE_NAME, operation, mapNamePartition);

        try {
            loadMapFuture.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }

        waitUntilLoaded();
    }

    /**
     * Maps keys to corresponding partitions and sends operations to them.
     */
    protected void loadInternal(Iterable keys, boolean replaceExistingValues) {
        Iterable<Data> dataKeys = convertToData(keys);
        NodeEngine nodeEngine = getNodeEngine();
        Map<Integer, List<Data>> partitionIdToKeys = getPartitionIdToKeysMap(dataKeys);
        Iterable<Entry<Integer, List<Data>>> entries = partitionIdToKeys.entrySet();

        for (final Entry<Integer, List<Data>> entry : entries) {
            final Integer partitionId = entry.getKey();
            final List<Data> correspondingKeys = entry.getValue();
            final Operation operation = createLoadAllOperation(correspondingKeys, replaceExistingValues);
            nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
        }
        waitUntilLoaded();
    }

    private <K> Iterable<Data> convertToData(Iterable<K> keys) {
        return IterableUtil.map(nullToEmpty(keys), new IFunction<K, Data>() {
            public Data apply(K key) {
                return toData(key);
            }
        });
    }

    private Operation createLoadAllOperation(final List<Data> keys, boolean replaceExistingValues) {
        return new LoadAllOperation(name, keys, replaceExistingValues);
    }

    protected Data removeInternal(Data key) {
        RemoveOperation operation = new RemoveOperation(name, key);
        Data previousValue = (Data) syncInvoke(key, operation);
        invalidateNearCache(key);
        return previousValue;
    }

    protected void deleteInternal(Data key) {
        DeleteOperation operation = new DeleteOperation(name, key);
        syncInvoke(key, operation);
        invalidateNearCache(key);
    }

    protected boolean removeInternal(final Data key, final Data value) {
        RemoveIfSameOperation operation = new RemoveIfSameOperation(name, key, value);
        boolean removed = (Boolean) syncInvoke(key, operation);
        invalidateNearCache(key);
        return removed;
    }

    protected boolean tryRemoveInternal(final Data key, final long timeout, final TimeUnit timeunit) {
        TryRemoveOperation operation = new TryRemoveOperation(name, key, getTimeInMillis(timeout, timeunit));
        boolean removed = (Boolean) syncInvoke(key, operation);
        invalidateNearCache(key);
        return removed;
    }

    protected ICompletableFuture<Data> removeAsyncInternal(final Data key) {
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        RemoveOperation operation = new RemoveOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            ICompletableFuture<Data> future
                    = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
            invalidateNearCache(key);
            return future;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected boolean containsKeyInternal(Data key) {
        if (isKeyInNearCache(key)) {
            return true;
        }
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        ContainsKeyOperation containsKeyOperation = new ContainsKeyOperation(name, key);
        containsKeyOperation.setThreadId(ThreadUtil.getThreadId());
        containsKeyOperation.setServiceName(SERVICE_NAME);
        try {
            Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, containsKeyOperation,
                    partitionId);
            return (Boolean) getService().getMapServiceContext().toObject(f.get());
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }


    private Object syncInvoke(Data key, KeyBasedMapOperation operation) {
        try {
            final long start = System.nanoTime();
            final Object result = doInvokeOperation(key, operation).get();
            updateStatistics(operation, start);
            return result;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private <E> InternalCompletableFuture<E> asyncInvoke(Data key, final KeyBasedMapOperation operation) {
        final long start = System.nanoTime();
        final InternalCompletableFuture<E> future = doInvokeOperation(key, operation);
        if (getMapConfig().isStatisticsEnabled()) {
            future.andThen(new ExecutionCallback<E>() {
                @Override public void onResponse(E response) {
                    updateStatistics(operation, start);
                }
                @Override public void onFailure(Throwable t) {
                    updateStatistics(operation, start);
                }
            });
        }
        return future;
    }

    private <E> InternalCompletableFuture<E> doInvokeOperation(Data key, final KeyBasedMapOperation operation) {
        operation.setThreadId(ThreadUtil.getThreadId());
        final NodeEngine nodeEngine = getNodeEngine();
        final InvocationBuilder invocationBuilder = nodeEngine.getOperationService()
                .createInvocationBuilder(SERVICE_NAME, operation, nodeEngine.getPartitionService().getPartitionId(key))
                .setResultDeserialized(false);
        return invocationBuilder.invoke();
    }

    private void updateStatistics(Operation operation, long startTimeNanos) {
        if (!getMapConfig().isStatisticsEnabled()) {
            return;
        }
        final long took = NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
        if (operation instanceof BasePutOperation) {
            localMapStats.incrementPuts(took);
        } else if (operation instanceof BaseRemoveOperation) {
            localMapStats.incrementRemoves(took);
        } else if (operation instanceof GetOperation) {
            localMapStats.incrementGets(took);
        }
    }

    public void waitUntilLoaded() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            OperationService operationService = nodeEngine.getOperationService();
            OperationFactory opFactory = new PartitionCheckIfLoadedOperationFactory(name);

            Map<Integer, Object> results;
            Collection<Integer> mapNamePartition = getPartitionsForKeys(singleton(toData(name)));

            results = operationService.invokeOnPartitions(SERVICE_NAME, opFactory, mapNamePartition);
            waitAllTrue(results);

            results = operationService.invokeOnAllPartitions(SERVICE_NAME, opFactory);
            waitAllTrue(results);

        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private void waitAllTrue(Map<Integer, Object> results)
            throws InterruptedException {
        Iterator<Entry<Integer, Object>> iterator = results.entrySet().iterator();
        boolean isFinished = false;
        final Set<Integer> retrySet = new HashSet<Integer>();
        while (!isFinished) {
            while (iterator.hasNext()) {
                final Entry<Integer, Object> entry = iterator.next();
                if (Boolean.TRUE.equals(entry.getValue())) {
                    iterator.remove();
                } else {
                    retrySet.add(entry.getKey());
                }
            }
            if (retrySet.size() > 0) {
                results = retryPartitions(retrySet);
                iterator = results.entrySet().iterator();
                final int oneSecond = 1000;
                Thread.sleep(oneSecond);
                retrySet.clear();
            } else {
                isFinished = true;
            }
        }
    }

    private Map<Integer, Object> retryPartitions(Collection partitions) {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            final Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnPartitions(SERVICE_NAME, new PartitionCheckIfLoadedOperationFactory(name), partitions);
            return results;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public int size() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new SizeOperationFactory(name));
            int total = 0;
            for (Object result : results.values()) {
                Integer size = (Integer) getService().getMapServiceContext().toObject(result);
                total += size;
            }
            return total;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public boolean containsValueInternal(Data dataValue) {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new ContainsValueOperationFactory(name, dataValue));
            for (Object result : results.values()) {
                Boolean contains = (Boolean) getService().getMapServiceContext().toObject(result);
                if (contains) {
                    return true;
                }
            }
            return false;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public boolean isEmpty() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            //TODO: We don't need to wait for all to complete, as soon as there is one future returning to false
            //we can stop. Also there is no need to make use of isEmptyOperation; just use size. This reduces the
            //amount of code.
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME,
                            new IsEmptyOperationFactory(name));
            for (Object result : results.values()) {
                if (!(Boolean) getService().getMapServiceContext().toObject(result)) {
                    return false;
                }
            }
            return true;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Map<Object, Object> getAllObjectInternal(final Set<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }
        final NodeEngine nodeEngine = getNodeEngine();
        final MapService mapService = getService();
        Map<Object, Object> result = new HashMap<Object, Object>();
        final boolean nearCacheEnabled = getMapConfig().isNearCacheEnabled();
        if (nearCacheEnabled) {
            getFromNearCache(result, keys);
        }
        if (keys.isEmpty()) {
            return result;
        }
        Collection<Integer> partitions = getPartitionsForKeys(keys);
        Map<Integer, Object> responses;
        try {
            responses = nodeEngine.getOperationService()
                    .invokeOnPartitions(SERVICE_NAME, new MapGetAllOperationFactory(name, keys), partitions);
            for (Object response : responses.values()) {
                Set<Map.Entry<Data, Data>> entries
                        = ((MapEntrySet) mapService.getMapServiceContext().toObject(response)).getEntrySet();
                for (Entry<Data, Data> entry : entries) {
                    result.put(mapService.getMapServiceContext().toObject(entry.getKey()),
                            mapService.getMapServiceContext().toObject(entry.getValue()));
                    if (nearCacheEnabled && (cachingLocalEntries() || keyIsRemotelyOwned(entry.getKey()))) {
                        getNearCacheProvider().putNearCache(name, entry.getKey(), entry.getValue());
                    }
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

        return result;
    }

    private Collection<Integer> getPartitionsForKeys(Set<Data> keys) {

        int partitions = partitionService.getPartitionCount();
        //todo: is there better way to estimate size?
        int capacity = Math.min(partitions, keys.size());
        Set<Integer> partitionIds = new HashSet<Integer>(capacity);

        Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext() && partitionIds.size() < partitions) {
            Data key = iterator.next();
            partitionIds.add(partitionService.getPartitionId(key));
        }
        return partitionIds;
    }

    private Map<Integer, List<Data>> getPartitionIdToKeysMap(Iterable<Data> keys) {
        if (keys == null) {
            return Collections.emptyMap();
        }
        final InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        final Map<Integer, List<Data>> idToKeys = new HashMap<Integer, List<Data>>();
        for (Data key : keys) {
            final int partitionId = partitionService.getPartitionId(key);
            List<Data> keyList = idToKeys.get(partitionId);
            if (keyList == null) {
                keyList = new ArrayList<Data>();
                idToKeys.put(partitionId, keyList);
            }
            keyList.add(key);
        }
        return idToKeys;
    }

    protected void putAllInternal(final Map<? extends Object, ? extends Object> entries) {
        final NodeEngine nodeEngine = getNodeEngine();
        final MapService mapService = getService();
        int factor = 3;
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        OperationService operationService = nodeEngine.getOperationService();
        int partitionCount = partitionService.getPartitionCount();
        boolean tooManyEntries = entries.size() > (partitionCount * factor);
        try {
            if (tooManyEntries) {
                List<Future> futures = new LinkedList<Future>();
                Map<Integer, MapEntrySet> entryMap
                        = new HashMap<Integer, MapEntrySet>(nodeEngine.getPartitionService().getPartitionCount());
                for (Entry entry : entries.entrySet()) {
                    checkNotNull(entry.getKey(), NULL_KEY_IS_NOT_ALLOWED);
                    checkNotNull(entry.getValue(), NULL_VALUE_IS_NOT_ALLOWED);

                    int partitionId = partitionService.getPartitionId(entry.getKey());
                    if (!entryMap.containsKey(partitionId)) {
                        entryMap.put(partitionId, new MapEntrySet());
                    }
                    entryMap.get(partitionId).add(
                            new AbstractMap.SimpleImmutableEntry<Data, Data>(mapService.getMapServiceContext().toData(
                                    entry.getKey(),
                                    partitionStrategy),
                                    mapService.getMapServiceContext()
                                            .toData(entry.getValue())
                            ));
                }

                for (final Map.Entry<Integer, MapEntrySet> entry : entryMap.entrySet()) {
                    final Integer partitionId = entry.getKey();
                    final PutAllOperation op = new PutAllOperation(name, entry.getValue());
                    op.setPartitionId(partitionId);
                    futures.add(operationService.invokeOnPartition(SERVICE_NAME, op, partitionId));
                }

                for (Future future : futures) {
                    future.get();
                }

            } else {
                for (Entry entry : entries.entrySet()) {
                    checkNotNull(entry.getKey(), NULL_KEY_IS_NOT_ALLOWED);
                    checkNotNull(entry.getValue(), NULL_VALUE_IS_NOT_ALLOWED);

                    putInternal(mapService.getMapServiceContext().toData(entry.getKey(), partitionStrategy),
                            mapService.getMapServiceContext().toData(entry.getValue()),
                            -1,
                            TimeUnit.MILLISECONDS);
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void flush() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            // todo add a feature to mancenter to sync cache to db completely
            nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME,
                            new BinaryOperationFactory(new MapFlushOperation(name), nodeEngine));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void clearInternal() {
        final String mapName = name;
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            ClearOperation clearOperation = new ClearOperation(mapName);
            clearOperation.setServiceName(SERVICE_NAME);
            final Map<Integer, Object> resultMap = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new BinaryOperationFactory(clearOperation, nodeEngine));

            int numberOfAffectedEntries = 0;
            for (Object o : resultMap.values()) {
                numberOfAffectedEntries += (Integer) o;
            }
            if (numberOfAffectedEntries > 0) {
                publishMapEvent(numberOfAffectedEntries, EntryEventType.CLEAR_ALL);
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public String addMapInterceptorInternal(MapInterceptor interceptor) {
        final NodeEngine nodeEngine = getNodeEngine();
        final MapService mapService = getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        if (interceptor instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) interceptor).setHazelcastInstance(nodeEngine.getHazelcastInstance());
        }
        String id = mapServiceContext.generateInterceptorId(name, interceptor);
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            try {
                Future f = nodeEngine.getOperationService()
                        .invokeOnTarget(SERVICE_NAME, new AddInterceptorOperation(id, interceptor, name),
                                member.getAddress());
                f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
        return id;
    }

    public void removeMapInterceptorInternal(String id) {
        final NodeEngine nodeEngine = getNodeEngine();
        final MapService mapService = getService();
        mapService.getMapServiceContext().removeInterceptor(name, id);
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (Member member : members) {
            try {
                if (member.localMember()) {
                    continue;
                }
                MemberImpl memberImpl = (MemberImpl) member;
                Future f = nodeEngine.getOperationService()
                        .invokeOnTarget(SERVICE_NAME, new RemoveInterceptorOperation(name, id), memberImpl.getAddress());
                f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    public String addLocalEntryListenerInternal(final Object listener) {
        final MapService mapService = getService();
        return mapService.getMapServiceContext().addLocalEventListener(listener, name);
    }

    public String addLocalEntryListenerInternal(Object listener, Predicate predicate,
                                                final Data key, boolean includeValue) {

        final MapService mapService = getService();
        EventFilter eventFilter = new QueryEventFilter(includeValue, key, predicate);
        return mapService.getMapServiceContext().addLocalEventListener(listener, eventFilter, name);
    }

    protected String addEntryListenerInternal(
            final Object listener, final Data key, final boolean includeValue) {
        EventFilter eventFilter = new EntryEventFilter(includeValue, key);
        final MapService mapService = getService();
        return mapService.getMapServiceContext().addEventListener(listener, eventFilter, name);
    }

    protected String addEntryListenerInternal(
            Object listener, Predicate predicate, final Data key, final boolean includeValue) {
        EventFilter eventFilter = new QueryEventFilter(includeValue, key, predicate);
        final MapService mapService = getService();
        return mapService.getMapServiceContext().addEventListener(listener, eventFilter, name);
    }

    protected boolean removeEntryListenerInternal(String id) {
        final MapService mapService = getService();
        return mapService.getMapServiceContext().removeEventListener(name, id);
    }

    protected String addPartitionLostListenerInternal(MapPartitionLostListener listener) {
        final MapService mapService = getService();
        return mapService.getMapServiceContext().addPartitionLostListener(listener, name);
    }

    protected boolean removePartitionLostListenerInternal(String id) {
        final MapService mapService = getService();
        return mapService.getMapServiceContext().removePartitionLostListener(name, id);
    }

    protected EntryView getEntryViewInternal(final Data key) {
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        GetEntryViewOperation operation = new GetEntryViewOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        operation.setServiceName(SERVICE_NAME);
        try {
            Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
            Object o = getService().getMapServiceContext().toObject(f.get());
            return (EntryView) o;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Data executeOnKeyInternal(Data key, EntryProcessor entryProcessor) {
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        EntryOperation operation = new EntryOperation(name, key, entryProcessor);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            Future future = nodeEngine.getOperationService()
                    .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                    .setResultDeserialized(false)
                    .invoke();
            final Data data = (Data) future.get();
            invalidateNearCache(key);
            return data;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Map executeOnKeysInternal(Set<Data> keys, EntryProcessor entryProcessor) {
        // todo: WHY are we not forwarding to executeOnKeysInternal(keys,entrprocessor, null)
        // or some other kind of fake callback. Now there is a lot of duplication
        Map result = new HashMap();
        final NodeEngine nodeEngine = getNodeEngine();
        final Collection<Integer> partitionsForKeys = getPartitionsForKeys(keys);
        try {
            MultipleEntryOperationFactory operationFactory = new MultipleEntryOperationFactory(name, keys, entryProcessor);
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnPartitions(SERVICE_NAME, operationFactory, partitionsForKeys);
            for (Object o : results.values()) {
                if (o != null) {
                    final MapService service = getService();
                    final MapEntrySet mapEntrySet = (MapEntrySet) o;
                    for (Entry<Data, Data> entry : mapEntrySet.getEntrySet()) {
                        result.put(service.getMapServiceContext().toObject(entry.getKey()),
                                service.getMapServiceContext().toObject(entry.getValue()));
                    }
                }
            }
            invalidateNearCache(keys);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }

    public ICompletableFuture executeOnKeyInternal(Data key, EntryProcessor entryProcessor, ExecutionCallback callback) {
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        EntryOperation operation = new EntryOperation(name, key, entryProcessor);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            if (callback == null) {
                return nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
            } else {
                ICompletableFuture future = nodeEngine.getOperationService()
                        .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setCallback(new MapExecutionCallbackAdapter(callback))
                        .invoke();
                invalidateNearCache(key);
                return future;
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    /**
     * {@link IMap#executeOnEntries(EntryProcessor)}
     */
    //todo: this method is untested
    public Map executeOnEntries(EntryProcessor entryProcessor) {
        return executeOnEntries(entryProcessor, TruePredicate.INSTANCE);
    }

    /**
     * {@link IMap#executeOnEntries(EntryProcessor, Predicate)}
     */
    //todo: this method is untested
    public Map executeOnEntries(EntryProcessor entryProcessor, Predicate predicate) {
        Map result = new HashMap();
        try {
            NodeEngine nodeEngine = getNodeEngine();
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME,
                            new PartitionWideEntryWithPredicateOperationFactory(name,
                                    entryProcessor,
                                    predicate)
                    );
            for (Object o : results.values()) {
                if (o != null) {
                    final MapService service = getService();
                    final MapEntrySet mapEntrySet = (MapEntrySet) o;
                    for (Entry<Data, Data> entry : mapEntrySet.getEntrySet()) {
                        final Data key = entry.getKey();
                        result.put(service.getMapServiceContext().toObject(key),
                                service.getMapServiceContext().toObject(entry.getValue()));
                        invalidateNearCache(key);
                    }
                }
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }

    protected Object toObject(Object obj) {
        return getService().getMapServiceContext().toObject(obj);
    }

    protected Data toData(Object obj) {
        return getService().getMapServiceContext().toData(obj);
    }

    protected Data toData(Object o, PartitioningStrategy partitioningStrategy) {
        return getService().getMapServiceContext().toData(o, partitioningStrategy);
    }

    protected Set queryLocal(final Predicate predicate, final IterationType iterationType, final boolean dataResult) {
        if (predicate instanceof PagingPredicate) {
            return getMapQuerySupport().queryLocalMemberWithPagingPredicate(name, (PagingPredicate) predicate, iterationType);
        }
        return getMapQuerySupport().queryLocalMember(name, predicate, iterationType, dataResult);
    }

    protected Set query(final Predicate predicate, final IterationType iterationType, final boolean dataResult) {
        if (predicate instanceof PagingPredicate) {
            return getMapQuerySupport().queryWithPagingPredicate(name, (PagingPredicate) predicate, iterationType);
        }
        return getMapQuerySupport().query(name, predicate, iterationType, dataResult);
    }

    public void addIndex(final String attribute, final boolean ordered) {
        final NodeEngine nodeEngine = getNodeEngine();
        if (attribute == null) {
            throw new IllegalArgumentException("Attribute name cannot be null");
        }
        try {
            AddIndexOperation addIndexOperation = new AddIndexOperation(name, attribute, ordered);
            nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new BinaryOperationFactory(addIndexOperation, nodeEngine));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public LocalMapStats getLocalMapStats() {
        return getService().getMapServiceContext().getLocalMapStatsProvider().createLocalMapStats(name);
    }

    private boolean isKeyInNearCache(Data key) {
        final MapService mapService = getService();
        final boolean nearCacheEnabled = getMapConfig().isNearCacheEnabled();
        if (nearCacheEnabled) {
            Object cached = mapService.getMapServiceContext().getNearCacheProvider().getFromNearCache(name, key);
            if (cached != null && !cached.equals(NearCache.NULL_OBJECT)) {
                return true;
            }
        }
        return false;
    }

    private void invalidateNearCache(Data key) {
        if (key == null) {
            return;
        }
        getNearCacheProvider().invalidateNearCache(name, key);
    }

    private void invalidateNearCache(Collection<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        getNearCacheProvider().invalidateNearCache(name, keys);
    }

    private void clearNearCache() {
        getNearCacheProvider().clearNearCache(name);
    }

    private void publishMapEvent(int numberOfAffectedEntries, EntryEventType eventType) {
        final MapService mapService = getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        mapEventPublisher.publishMapEvent(getNodeEngine().getThisAddress(), name, eventType, numberOfAffectedEntries);
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        long timeInMillis = timeunit.toMillis(time);
        if (time > 0 && timeInMillis == 0) {
            timeInMillis = 1;
        }
        return timeInMillis;
    }

    private MapContextQuerySupport getMapQuerySupport() {
        return getService().getMapServiceContext().getMapContextQuerySupport();
    }

    private NearCacheProvider getNearCacheProvider() {
        return getService().getMapServiceContext().getNearCacheProvider();
    }

    protected MapStore getMapStore() {
        final MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        final MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        return mapContainer.getMapStoreContext().getMapStoreWrapper();

    }

    private MapConfig getMapConfig() {
        final MapService mapService = getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        return mapContainer.getMapConfig();
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final String getServiceName() {
        return SERVICE_NAME;
    }

    private class MapExecutionCallbackAdapter implements Callback {

        private final ExecutionCallback executionCallback;

        public MapExecutionCallbackAdapter(ExecutionCallback executionCallback) {
            this.executionCallback = executionCallback;
        }

        @Override
        public void notify(Object response) {
            if (response instanceof Throwable) {
                executionCallback.onFailure((Throwable) response);
            } else {
                executionCallback.onResponse(getService().getMapServiceContext().toObject(response));
            }
        }
    }

    public PartitioningStrategy getPartitionStrategy() {
        return partitionStrategy;
    }
}

