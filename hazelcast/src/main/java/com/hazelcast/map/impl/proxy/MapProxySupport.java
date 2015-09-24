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
import com.hazelcast.concurrent.lock.LockServiceImpl;
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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.nearcache.NearCache;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
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
import com.hazelcast.map.impl.query.MapQueryEngine;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.BinaryOperationFactory;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterableUtil;
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.IterableUtil.nullToEmpty;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Math.min;
import static java.util.Collections.singleton;

abstract class MapProxySupport extends AbstractDistributedObject<MapService> implements InitializingObject {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    protected static final String NULL_PREDICATE_IS_NOT_ALLOWED = "Predicate should not be null!";
    protected static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";

    protected final String name;
    protected final LocalMapStatsImpl localMapStats;
    protected final LockProxySupport lockSupport;
    protected final PartitioningStrategy partitionStrategy;
    private final MapContainer mapContainer;
    private final OperationService operationService;
    private final SerializationService serializationService;
    private final NearCache nearCache;
    private final Address thisAddress;
    private final boolean statisticsEnabled;
    private MapServiceContext mapServiceContext;
    private InternalPartitionService partitionService;

    protected MapProxySupport(String name, MapService service, NodeEngine nodeEngine) {
        super(nodeEngine, service);
        this.name = name;

        this.mapServiceContext = service.getMapServiceContext();
        this.mapContainer = mapServiceContext.getMapContainer(name);
        this.partitionStrategy = mapServiceContext.getMapContainer(name).getPartitioningStrategy();
        this.localMapStats = mapServiceContext.getLocalMapStatsProvider().getLocalMapStatsImpl(name);
        this.partitionService = getNodeEngine().getPartitionService();
        this.operationService = nodeEngine.getOperationService();
        this.serializationService = nodeEngine.getSerializationService();
        this.lockSupport = new LockProxySupport(new DefaultObjectNamespace(MapService.SERVICE_NAME, name),
                LockServiceImpl.getMaxLeaseTimeInMillis(nodeEngine.getGroupProperties()));
        this.thisAddress = nodeEngine.getClusterService().getThisAddress();
        this.statisticsEnabled = mapContainer.getMapConfig().isStatisticsEnabled();

        if (mapContainer.isNearCacheEnabled()) {
            NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
            nearCache = nearCacheProvider.getNearCache(name);
        } else {
            nearCache = null;
        }
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
        MapConfig mapConfig = getMapConfig();

        for (EntryListenerConfig listenerConfig : mapConfig.getEntryListenerConfigs()) {
            MapListener listener = initializeListener(listenerConfig);
            if (listener != null) {
                if (listenerConfig.isLocal()) {
                    addLocalEntryListenerInternal(listener);
                } else {
                    addEntryListenerInternal(listener, null, listenerConfig.isIncludeValue());
                }
            }
        }

        for (MapPartitionLostListenerConfig listenerConfig : mapConfig.getPartitionLostListenerConfigs()) {
            MapPartitionLostListener listener = initializeListener(listenerConfig);
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

    // this operation returns the object in data format except
    // it is got from near-cache and near-cache memory format is object.
    protected Object getInternal(Data key) {
        // todo: why does this method not make use of getAsyncInternal and just do a get on it?
        // now there is a lot of duplication.
        if (nearCache != null) {
            Object fromNearCache = getFromNearCache(key);
            if (fromNearCache != null) {
                if (isCachedAsNullInNearCache(fromNearCache)) {
                    return null;
                }
                return fromNearCache;
            }
        }
        // todo action for read-backup true is not well tested.
        if (getMapConfig().isReadBackupData()) {
            Object fromBackup = readBackupDataOrNull(key);
            if (fromBackup != null) {
                return fromBackup;
            }
        }

        GetOperation operation = new GetOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        Data value = (Data) invokeOperation(key, operation);

        if (nearCache != null) {
            if (notOwnerPartitionForKey(key) || cacheKeyAnyway()) {
                return putNearCache(key, value);
            }
        }
        return value;
    }

    private boolean notOwnerPartitionForKey(Data key) {
        int partitionId = partitionService.getPartitionId(key);
        return !partitionService.getPartitionOwner(partitionId).equals(thisAddress);
    }

    private boolean cacheKeyAnyway() {
        return getMapConfig().getNearCacheConfig().isCacheLocalEntries();
    }

    private Object putNearCache(Data key, Data value) {
        return nearCache.put(key, value);
    }

    private Object getFromNearCache(Data key) {
        if (nearCache == null) {
            return null;
        }

        Object cached = nearCache.get(key);
        if (cached == null) {
            return null;
        }
        mapServiceContext.interceptAfterGet(name, cached);
        return cached;
    }

    private void getFromNearCache(Map<Object, Object> resultMap, Collection<Data> keys) {
        if (nearCache == null) {
            return;
        }

        Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            Data key = iterator.next();
            Object fromNearCache = getFromNearCache(key);
            if (fromNearCache == null) {
                continue;
            }
            if (!isCachedAsNullInNearCache(fromNearCache)) {
                resultMap.put(toObject(key), toObject(fromNearCache));
            }
            iterator.remove();
        }
    }

    private boolean isCachedAsNullInNearCache(Object cached) {
        if (cached == null) {
            return false;
        }
        return NearCache.NULL_OBJECT.equals(cached);
    }

    private Data readBackupDataOrNull(Data key) {
        int partitionId = partitionService.getPartitionId(key);
        InternalPartition partition = partitionService.getPartition(partitionId, false);
        if (!partition.isOwnerOrBackup(thisAddress)) {
            return null;
        }
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        RecordStore recordStore = partitionContainer.getExistingRecordStore(name);
        if (recordStore == null) {
            return null;
        }
        return recordStore.readBackupData(key);
    }

    protected ICompletableFuture<Data> getAsyncInternal(final Data key) {
        int partitionId = partitionService.getPartitionId(key);
        if (nearCache != null) {
            Object cached = nearCache.get(key);
            if (cached != null) {
                if (NearCache.NULL_OBJECT.equals(cached)) {
                    cached = null;
                }
                return new CompletedFuture<Data>(
                        getNodeEngine().getSerializationService(),
                        cached,
                        getNodeEngine().getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR));
            }
        }

        GetOperation op = new GetOperation(name, key);
        try {
            InternalCompletableFuture<Data> future = operationService.createInvocationBuilder(SERVICE_NAME, op, partitionId)
                    .setResultDeserialized(false)
                    .invoke();
            future.andThen(new ExecutionCallback<Data>() {
                @Override
                public void onResponse(Data response) {
                    if (nearCache != null) {
                        int partitionId = partitionService.getPartitionId(key);
                        if (!partitionService.getPartitionOwner(partitionId).equals(thisAddress)
                                || getMapConfig().getNearCacheConfig().isCacheLocalEntries()) {
                            nearCache.put(key, response);
                        }
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                }
            });
            return future;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Data putInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        PutOperation operation = new PutOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        Data previousValue = (Data) invokeOperation(key, operation);
        invalidateNearCache(key);
        return previousValue;
    }

    protected boolean tryPutInternal(Data key, Data value, long timeout, TimeUnit timeunit) {
        TryPutOperation operation = new TryPutOperation(name, key, value, getTimeInMillis(timeout, timeunit));
        boolean putSuccessful = (Boolean) invokeOperation(key, operation);
        invalidateNearCache(key);
        return putSuccessful;
    }

    protected Data putIfAbsentInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        PutIfAbsentOperation operation = new PutIfAbsentOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        Data previousValue = (Data) invokeOperation(key, operation);
        invalidateNearCache(key);
        return previousValue;
    }

    protected void putTransientInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        PutTransientOperation operation = new PutTransientOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        invokeOperation(key, operation);
        invalidateNearCache(key);
    }

    private Object invokeOperation(Data key, KeyBasedMapOperation operation) {
        int partitionId = partitionService.getPartitionId(key);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            Object result;
            if (statisticsEnabled) {
                long time = System.currentTimeMillis();
                Future f = operationService
                        .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setResultDeserialized(false)
                        .invoke();
                result = f.get();
                if (operation instanceof BasePutOperation) {
                    localMapStats.incrementPuts(System.currentTimeMillis() - time);
                } else if (operation instanceof BaseRemoveOperation) {
                    localMapStats.incrementRemoves(System.currentTimeMillis() - time);
                } else if (operation instanceof GetOperation) {
                    localMapStats.incrementGets(System.currentTimeMillis() - time);
                }
            } else {
                Future f = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setResultDeserialized(false).invoke();
                result = f.get();
            }
            return result;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected ICompletableFuture<Data> putAsyncInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        int partitionId = partitionService.getPartitionId(key);
        PutOperation operation = new PutOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            ICompletableFuture<Data> future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
            invalidateNearCache(key);
            return future;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected boolean replaceInternal(Data key, Data expect, Data update) {
        ReplaceIfSameOperation operation = new ReplaceIfSameOperation(name, key, expect, update);
        boolean replaceSuccessful = (Boolean) invokeOperation(key, operation);
        invalidateNearCache(key);
        return replaceSuccessful;
    }

    protected Data replaceInternal(Data key, Data value) {
        ReplaceOperation operation = new ReplaceOperation(name, key, value);
        Data result = (Data) invokeOperation(key, operation);
        invalidateNearCache(key);
        return result;
    }

    //warning: When UpdateEvent is fired it does *NOT* contain oldValue.
    //see this: https://github.com/hazelcast/hazelcast/pull/6088#issuecomment-136025968
    protected void setInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        SetOperation operation = new SetOperation(name, key, value, timeunit.toMillis(ttl));
        invokeOperation(key, operation);
        invalidateNearCache(key);
    }

    protected boolean evictInternal(Data key) {
        EvictOperation operation = new EvictOperation(name, key, false);
        boolean evictSuccess = (Boolean) invokeOperation(key, operation);
        invalidateNearCache(key);
        return evictSuccess;
    }

    protected void evictAllInternal() {
        try {
            clearNearCache();
            Operation operation = new EvictAllOperation(name);
            Map<Integer, Object> resultMap = operationService.invokeOnAllPartitions(SERVICE_NAME,
                    new BinaryOperationFactory(operation, getNodeEngine()));

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
     *
     * @param keys
     * @param replaceExistingValues
     */
    protected void loadInternal(Iterable keys, boolean replaceExistingValues) {
        Iterable<Data> dataKeys = convertToData(keys);
        Map<Integer, List<Data>> partitionIdToKeys = getPartitionIdToKeysMap(dataKeys);
        Iterable<Entry<Integer, List<Data>>> entries = partitionIdToKeys.entrySet();

        for (Entry<Integer, List<Data>> entry : entries) {
            Integer partitionId = entry.getKey();
            List<Data> correspondingKeys = entry.getValue();
            Operation operation = createLoadAllOperation(correspondingKeys, replaceExistingValues);
            operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
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

    private Operation createLoadAllOperation(List<Data> keys, boolean replaceExistingValues) {
        return new LoadAllOperation(name, keys, replaceExistingValues);
    }

    protected Data removeInternal(Data key) {
        RemoveOperation operation = new RemoveOperation(name, key);
        Data previousValue = (Data) invokeOperation(key, operation);
        invalidateNearCache(key);
        return previousValue;
    }

    protected void deleteInternal(Data key) {
        DeleteOperation operation = new DeleteOperation(name, key);
        invokeOperation(key, operation);
        invalidateNearCache(key);
    }

    protected boolean removeInternal(Data key, Data value) {
        RemoveIfSameOperation operation = new RemoveIfSameOperation(name, key, value);
        boolean removed = (Boolean) invokeOperation(key, operation);
        invalidateNearCache(key);
        return removed;
    }

    protected boolean tryRemoveInternal(Data key, long timeout, TimeUnit timeunit) {
        TryRemoveOperation operation = new TryRemoveOperation(name, key, getTimeInMillis(timeout, timeunit));
        boolean removed = (Boolean) invokeOperation(key, operation);
        invalidateNearCache(key);
        return removed;
    }

    protected ICompletableFuture<Data> removeAsyncInternal(Data key) {
        int partitionId = partitionService.getPartitionId(key);
        RemoveOperation operation = new RemoveOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            ICompletableFuture<Data> future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
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

        int partitionId = partitionService.getPartitionId(key);
        ContainsKeyOperation containsKeyOperation = new ContainsKeyOperation(name, key);
        containsKeyOperation.setThreadId(ThreadUtil.getThreadId());
        containsKeyOperation.setServiceName(SERVICE_NAME);
        try {
            Future f = operationService.invokeOnPartition(SERVICE_NAME, containsKeyOperation, partitionId);
            return (Boolean) toObject(f.get());
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void waitUntilLoaded() {
        try {
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

    private void waitAllTrue(Map<Integer, Object> results) throws InterruptedException {
        Iterator<Entry<Integer, Object>> iterator = results.entrySet().iterator();
        boolean isFinished = false;
        Set<Integer> retrySet = new HashSet<Integer>();
        while (!isFinished) {
            while (iterator.hasNext()) {
                Entry<Integer, Object> entry = iterator.next();
                if (Boolean.TRUE.equals(entry.getValue())) {
                    iterator.remove();
                } else {
                    retrySet.add(entry.getKey());
                }
            }
            if (retrySet.size() > 0) {
                results = retryPartitions(retrySet);
                iterator = results.entrySet().iterator();
                TimeUnit.SECONDS.sleep(1);
                retrySet.clear();
            } else {
                isFinished = true;
            }
        }
    }

    private Map<Integer, Object> retryPartitions(Collection partitions) {
        try {
            Map<Integer, Object> results = operationService.invokeOnPartitions(
                    SERVICE_NAME, new PartitionCheckIfLoadedOperationFactory(name), partitions);
            return results;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public int size() {
        try {
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, new SizeOperationFactory(name));
            int total = 0;
            for (Object result : results.values()) {
                Integer size = (Integer) toObject(result);
                total += size;
            }
            return total;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public boolean containsValueInternal(Data dataValue) {
        try {
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(
                    SERVICE_NAME, new ContainsValueOperationFactory(name, dataValue));
            for (Object result : results.values()) {
                Boolean contains = (Boolean) toObject(result);
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
        try {
            //TODO: We don't need to wait for all to complete, as soon as there is one future returning to false
            //we can stop. Also there is no need to make use of isEmptyOperation; just use size. This reduces the
            //amount of code.
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(
                    SERVICE_NAME, new IsEmptyOperationFactory(name));
            for (Object result : results.values()) {
                if (!(Boolean) toObject(result)) {
                    return false;
                }
            }
            return true;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Map<Object, Object> getAllObjectInternal(Set<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Object, Object> result = new HashMap<Object, Object>();
        if (nearCache != null) {
            getFromNearCache(result, keys);
        }
        if (keys.isEmpty()) {
            return result;
        }
        Collection<Integer> partitions = getPartitionsForKeys(keys);
        Map<Integer, Object> responses;
        try {
            responses = operationService.invokeOnPartitions(SERVICE_NAME, new MapGetAllOperationFactory(name, keys), partitions);
            for (Object response : responses.values()) {
                Set<Map.Entry<Data, Data>> entries = ((MapEntrySet) toObject(response)).getEntrySet();
                for (Entry<Data, Data> entry : entries) {
                    result.put(toObject(entry.getKey()), toObject(entry.getValue()));
                    if (nearCache != null) {
                        if (notOwnerPartitionForKey(entry.getKey()) || cacheKeyAnyway()) {
                            putNearCache(entry.getKey(), entry.getValue());
                        }
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
        int capacity = min(partitions, keys.size());
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

        Map<Integer, List<Data>> idToKeys = new HashMap<Integer, List<Data>>();
        for (Data key : keys) {
            int partitionId = partitionService.getPartitionId(key);
            List<Data> keyList = idToKeys.get(partitionId);
            if (keyList == null) {
                keyList = new ArrayList<Data>();
                idToKeys.put(partitionId, keyList);
            }
            keyList.add(key);
        }
        return idToKeys;
    }

    /**
     * This Operation will first group all puts per partition and then send a PutAllOperation per partition. So if there are e.g.
     * 5 keys for a single partition, then instead of having 5 remote invocations, there will be only 1 remote invocation.
     * <p/>
     * If there are multiple puts for different partitions on the same member, they are executed as different remote operations.
     * Probably this can be optimized in the future by making use of an PartitionIterating operation.
     *
     * @param entries
     */
    protected void putAllInternal(Map<? extends Object, ? extends Object> entries) {
        int partitionCount = partitionService.getPartitionCount();

        try {
            List<Future> futures = new ArrayList<Future>(partitionCount);
            MapEntrySet[] entrySetPerPartition = new MapEntrySet[partitionCount];

            // first we fill entrySetPerPartition
            for (Entry entry : entries.entrySet()) {
                checkNotNull(entry.getKey(), NULL_KEY_IS_NOT_ALLOWED);
                checkNotNull(entry.getValue(), NULL_VALUE_IS_NOT_ALLOWED);

                int partitionId = partitionService.getPartitionId(entry.getKey());
                MapEntrySet entrySet = entrySetPerPartition[partitionId];
                if (entrySet == null) {
                    entrySet = new MapEntrySet();
                    entrySetPerPartition[partitionId] = entrySet;
                }

                Data keyData = mapServiceContext.toData(entry.getKey(), partitionStrategy);
                Data valueData = mapServiceContext.toData(entry.getValue());
                entrySet.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(keyData, valueData));
            }

            // then we invoke the operations
            for (int partitionId = 0; partitionId < entrySetPerPartition.length; partitionId++) {
                MapEntrySet entrySet = entrySetPerPartition[partitionId];
                if (entrySet != null) {
                    // If there is a single entry, we could make use of a PutOperation since that is a bit cheaper
                    Future f = createPutAllOperationFuture(name, entrySet, partitionId);
                    futures.add(f);
                }
            }

            // then we sync on completion of these operations
            for (Future future : futures) {
                future.get();
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Future createPutAllOperationFuture(final String name, MapEntrySet entrySet, int partitionId) {
        Operation op = new PutAllOperation(name, entrySet).setPartitionId(partitionId);
        final long size = entrySet.getEntrySet().size();
        final long time = System.currentTimeMillis();
        InternalCompletableFuture<Object> f = operationService.invokeOnPartition(SERVICE_NAME, op, partitionId);
        f.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                LocalMapStatsProvider localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
                LocalMapStatsImpl localMapStats = localMapStatsProvider.getLocalMapStatsImpl(name);
                long currentTime = System.currentTimeMillis();
                localMapStats.incrementPuts(size, currentTime - time);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        });
        return f;
    }

    public void flush() {
        try {
            // todo add a feature to mancenter to sync cache to db completely
            operationService.invokeOnAllPartitions(SERVICE_NAME,
                    new BinaryOperationFactory(new MapFlushOperation(name), getNodeEngine()));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void clearInternal() {
        try {
            ClearOperation clearOperation = new ClearOperation(name);
            clearOperation.setServiceName(SERVICE_NAME);
            Map<Integer, Object> resultMap = operationService.invokeOnAllPartitions(
                    SERVICE_NAME, new BinaryOperationFactory(clearOperation, getNodeEngine()));

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
        NodeEngine nodeEngine = getNodeEngine();
        if (interceptor instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) interceptor).setHazelcastInstance(nodeEngine.getHazelcastInstance());
        }
        String id = mapServiceContext.generateInterceptorId(name, interceptor);
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        for (Member member : members) {
            try {
                AddInterceptorOperation op = new AddInterceptorOperation(id, interceptor, name);
                Future f = operationService.invokeOnTarget(SERVICE_NAME, op, member.getAddress());
                f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
        return id;
    }

    public void removeMapInterceptorInternal(String id) {
        NodeEngine nodeEngine = getNodeEngine();
        mapServiceContext.removeInterceptor(name, id);
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        for (Member member : members) {
            try {
                if (member.localMember()) {
                    continue;
                }
                RemoveInterceptorOperation op = new RemoveInterceptorOperation(name, id);
                Future f = operationService.invokeOnTarget(SERVICE_NAME, op, member.getAddress());
                f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    public String addLocalEntryListenerInternal(Object listener) {
        return mapServiceContext.addLocalEventListener(listener, name);
    }

    public String addLocalEntryListenerInternal(Object listener, Predicate predicate, Data key, boolean includeValue) {
        EventFilter eventFilter = new QueryEventFilter(includeValue, key, predicate);
        return mapServiceContext.addLocalEventListener(listener, eventFilter, name);
    }

    protected String addEntryListenerInternal(Object listener, Data key, boolean includeValue) {
        EventFilter eventFilter = new EntryEventFilter(includeValue, key);
        return mapServiceContext.addEventListener(listener, eventFilter, name);
    }

    protected String addEntryListenerInternal(Object listener, Predicate predicate, Data key, boolean includeValue) {
        EventFilter eventFilter = new QueryEventFilter(includeValue, key, predicate);
        return mapServiceContext.addEventListener(listener, eventFilter, name);
    }

    protected boolean removeEntryListenerInternal(String id) {
        return mapServiceContext.removeEventListener(name, id);
    }

    protected String addPartitionLostListenerInternal(MapPartitionLostListener listener) {
        return mapServiceContext.addPartitionLostListener(listener, name);
    }

    protected boolean removePartitionLostListenerInternal(String id) {
        return mapServiceContext.removePartitionLostListener(name, id);
    }

    protected EntryView getEntryViewInternal(Data key) {
        int partitionId = partitionService.getPartitionId(key);
        GetEntryViewOperation operation = new GetEntryViewOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        operation.setServiceName(SERVICE_NAME);
        try {
            Future f = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
            Object o = toObject(f.get());
            return (EntryView) o;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Data executeOnKeyInternal(Data key, EntryProcessor entryProcessor) {
        int partitionId = partitionService.getPartitionId(key);
        EntryOperation operation = new EntryOperation(name, key, entryProcessor);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            Future future = operationService
                    .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                    .setResultDeserialized(false)
                    .invoke();
            Data data = (Data) future.get();
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
        Collection<Integer> partitionsForKeys = getPartitionsForKeys(keys);
        try {
            MultipleEntryOperationFactory operationFactory = new MultipleEntryOperationFactory(name, keys, entryProcessor);
            Map<Integer, Object> results = operationService.invokeOnPartitions(SERVICE_NAME, operationFactory, partitionsForKeys);
            for (Object o : results.values()) {
                if (o != null) {
                    MapEntrySet mapEntrySet = (MapEntrySet) o;
                    for (Entry<Data, Data> entry : mapEntrySet.getEntrySet()) {
                        result.put(toObject(entry.getKey()), toObject(entry.getValue()));
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
        int partitionId = partitionService.getPartitionId(key);
        EntryOperation operation = new EntryOperation(name, key, entryProcessor);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            if (callback == null) {
                return operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
            } else {
                ICompletableFuture future = operationService
                        .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setExecutionCallback(new MapExecutionCallbackAdapter(callback))
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
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME,
                    new PartitionWideEntryWithPredicateOperationFactory(name, entryProcessor, predicate));
            for (Object o : results.values()) {
                if (o != null) {
                    MapEntrySet mapEntrySet = (MapEntrySet) o;
                    for (Entry<Data, Data> entry : mapEntrySet.getEntrySet()) {
                        Data key = entry.getKey();
                        result.put(toObject(key), toObject(entry.getValue()));
                        invalidateNearCache(key);
                    }
                }
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }

    protected <T> T toObject(Object obj) {
        return serializationService.toObject(obj);
    }

    protected Data toData(Object obj) {
        return serializationService.toData(obj);
    }

    protected Data toData(Object o, PartitioningStrategy partitioningStrategy) {
        return serializationService.toData(o, partitioningStrategy);
    }

    public void addIndex(String attribute, boolean ordered) {
        if (attribute == null) {
            throw new IllegalArgumentException("Attribute name cannot be null");
        }
        try {
            AddIndexOperation addIndexOperation = new AddIndexOperation(name, attribute, ordered);
            operationService.invokeOnAllPartitions(SERVICE_NAME, new BinaryOperationFactory(addIndexOperation, getNodeEngine()));
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public LocalMapStats getLocalMapStats() {
        return mapServiceContext.getLocalMapStatsProvider().createLocalMapStats(name);
    }

    private boolean isKeyInNearCache(Data key) {
        if (nearCache != null) {
            Object cached = nearCache.get(key);
            if (cached != null && !cached.equals(NearCache.NULL_OBJECT)) {
                return true;
            }
        }
        return false;
    }

    private void invalidateNearCache(Data key) {
        if (key != null && nearCache != null) {
            nearCache.invalidate(key);
        }
    }

    private void invalidateNearCache(Collection<Data> keys) {
        if (nearCache != null) {
            nearCache.invalidate(keys);
        }
    }

    private void clearNearCache() {
        if (nearCache != null) {
            nearCache.clear();
        }
    }

    private void publishMapEvent(int numberOfAffectedEntries, EntryEventType eventType) {
        MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        mapEventPublisher.publishMapEvent(thisAddress, name, eventType, numberOfAffectedEntries);
    }

    protected long getTimeInMillis(long time, TimeUnit timeunit) {
        long timeInMillis = timeunit.toMillis(time);
        if (time > 0 && timeInMillis == 0) {
            timeInMillis = 1;
        }
        return timeInMillis;
    }

    protected MapQueryEngine getMapQueryEngine() {
        return mapServiceContext.getMapQueryEngine();
    }

    protected MapStore getMapStore() {
        return mapContainer.getMapStoreContext().getMapStoreWrapper();
    }

    private MapConfig getMapConfig() {
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

    private class MapExecutionCallbackAdapter implements ExecutionCallback {

        private final ExecutionCallback executionCallback;

        public MapExecutionCallbackAdapter(ExecutionCallback executionCallback) {
            this.executionCallback = executionCallback;
        }

        @Override
        public void onResponse(Object response) {
            executionCallback.onResponse(toObject(response));
        }

        @Override
        public void onFailure(Throwable t) {
            executionCallback.onFailure(t);
        }
    }

    public PartitioningStrategy getPartitionStrategy() {
        return partitionStrategy;
    }
}

