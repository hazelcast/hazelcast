/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.operation.AddIndexOperation;
import com.hazelcast.map.impl.operation.AddInterceptorOperation;
import com.hazelcast.map.impl.operation.AwaitMapFlushOperation;
import com.hazelcast.map.impl.operation.IsEmptyOperationFactory;
import com.hazelcast.map.impl.operation.IsKeyLoadFinishedOperation;
import com.hazelcast.map.impl.operation.IsPartitionLoadedOperationFactory;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.operation.RemoveInterceptorOperation;
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
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.impl.BinaryOperationFactory;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.IterableUtil;
import com.hazelcast.util.MutableLong;
import com.hazelcast.util.ThreadUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Arrays;
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

import static com.hazelcast.config.MapIndexConfig.validateIndexAttribute;
import static com.hazelcast.core.EntryEventType.CLEAR_ALL;
import static com.hazelcast.map.impl.EntryRemovingProcessor.ENTRY_REMOVING_PROCESSOR;
import static com.hazelcast.map.impl.LocalMapStatsProvider.EMPTY_LOCAL_MAP_STATS;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.IterableUtil.nullToEmpty;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Math.ceil;
import static java.lang.Math.log10;
import static java.lang.Math.min;

abstract class MapProxySupport extends AbstractDistributedObject<MapService> implements InitializingObject {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    protected static final String NULL_PREDICATE_IS_NOT_ALLOWED = "Predicate should not be null!";
    protected static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";
    protected static final String NULL_AGGREGATOR_IS_NOT_ALLOWED = "Aggregator should not be null!";
    protected static final String NULL_PROJECTION_IS_NOT_ALLOWED = "Projection should not be null!";

    private static final int INITIAL_WAIT_LOAD_SLEEP_MILLIS = 10;
    private static final int MAXIMAL_WAIT_LOAD_SLEEP_MILLIS = 1000;

    /**
     * Defines the batch size for operations of {@link IMap#putAll(Map)} calls.
     * <p>
     * A value of {@code 0} disables the batching and will send a single operation per member with all map entries.
     * <p>
     * If you set this value too high, you may ran into OOME or blocked network pipelines due to huge operations.
     * If you set this value too low, you will lower the performance of the putAll() operation.
     */
    @Beta
    private static final HazelcastProperty MAP_PUT_ALL_BATCH_SIZE
            = new HazelcastProperty("hazelcast.map.put.all.batch.size", 0);

    /**
     * Defines the initial size of entry arrays per partition for {@link IMap#putAll(Map)} calls.
     * <p>
     * {@link IMap#putAll(Map)} splits up the entries of the user input map per partition,
     * to eventually send the entries the correct target nodes.
     * So the method creates multiple arrays with map entries per partition.
     * This value determines how the initial size of these arrays is calculated.
     * <p>
     * The default value of {@code 0} uses an educated guess, depending on the map size, which is a good overall strategy.
     * If you insert entries which don't match a normal partition distribution you should configure this factor.
     * The initial size is calculated by this formula:
     * {@code initialSize = ceil(MAP_PUT_ALL_INITIAL_SIZE_FACTOR * map.size() / PARTITION_COUNT)}
     * <p>
     * As a rule of thumb you can try the following values:
     * <ul>
     * <li>{@code 10.0} for map sizes up to 500 entries</li>
     * <li>{@code 5.0} for map sizes between 500 and 5000 entries</li>
     * <li>{@code 1.5} for map sizes between up to 50000 entries</li>
     * <li>{@code 1.0} for map sizes beyond 50000 entries</li>
     * </ul>
     * <p>
     * If you set this value too high, you will waste memory.
     * If you set this value too low, you will suffer from expensive {@link java.util.Arrays#copyOf} calls.
     */
    @Beta
    private static final HazelcastProperty MAP_PUT_ALL_INITIAL_SIZE_FACTOR
            = new HazelcastProperty("hazelcast.map.put.all.initial.size.factor", 0);

    protected final String name;
    protected final LocalMapStatsImpl localMapStats;
    protected final LockProxySupport lockSupport;
    protected final PartitioningStrategy partitionStrategy;
    protected final MapServiceContext mapServiceContext;
    protected final IPartitionService partitionService;
    protected final Address thisAddress;
    protected final OperationService operationService;
    protected final SerializationService serializationService;
    protected final boolean statisticsEnabled;
    protected final MapConfig mapConfig;
    protected final String localMemberUuid;

    // not final for testing purposes
    protected MapOperationProvider operationProvider;

    private final int putAllBatchSize;
    private final float putAllInitialSizeFactor;

    protected MapProxySupport(String name, MapService service, NodeEngine nodeEngine, MapConfig mapConfig) {
        super(nodeEngine, service);
        this.name = name;

        HazelcastProperties properties = nodeEngine.getProperties();

        this.mapServiceContext = service.getMapServiceContext();
        this.mapConfig = mapConfig;
        this.partitionStrategy = mapServiceContext.getPartitioningStrategy(mapConfig.getName(),
                mapConfig.getPartitioningStrategyConfig());
        this.localMapStats = mapServiceContext.getLocalMapStatsProvider().getLocalMapStatsImpl(name);
        this.partitionService = getNodeEngine().getPartitionService();
        this.lockSupport = new LockProxySupport(new DefaultObjectNamespace(SERVICE_NAME, name),
                LockServiceImpl.getMaxLeaseTimeInMillis(properties));
        this.operationProvider = mapServiceContext.getMapOperationProvider(mapConfig);
        this.operationService = nodeEngine.getOperationService();
        this.serializationService = nodeEngine.getSerializationService();
        this.thisAddress = nodeEngine.getClusterService().getThisAddress();
        this.statisticsEnabled = mapConfig.isStatisticsEnabled();
        // it is safe to cache local member uuid here because cluster start must be already completed
        this.localMemberUuid = mapServiceContext.getNodeEngine().getLocalMember().getUuid();

        this.putAllBatchSize = properties.getInteger(MAP_PUT_ALL_BATCH_SIZE);
        this.putAllInitialSizeFactor = properties.getFloat(MAP_PUT_ALL_INITIAL_SIZE_FACTOR);
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
        T listener = getListenerImplOrNull(listenerConfig);

        if (listener instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) listener).setHazelcastInstance(getNodeEngine().getHazelcastInstance());
        }

        return listener;
    }

    @SuppressWarnings("unchecked")
    private <T extends EventListener> T getListenerImplOrNull(ListenerConfig listenerConfig) {
        EventListener implementation = listenerConfig.getImplementation();
        if (implementation != null) {

            // for this instanceOf check please see EntryListenerConfig#toEntryListener
            if (implementation instanceof EntryListenerConfig.MapListenerToEntryListenerAdapter) {
                return (T) ((EntryListenerConfig.MapListenerToEntryListenerAdapter) implementation).getMapListener();
            }

            return (T) implementation;
        }

        String className = listenerConfig.getClassName();
        if (className != null) {
            try {
                ClassLoader configClassLoader = getNodeEngine().getConfigClassLoader();
                return ClassLoaderUtil.newInstance(configClassLoader, className);
            } catch (Exception e) {
                throw rethrow(e);
            }
        }

        // returning null to preserve previous behavior
        return null;
    }

    protected Object getInternal(Data key) {
        // todo action for read-backup true is not well tested.
        if (getMapConfig().isReadBackupData()) {
            Object fromBackup = readBackupDataOrNull(key);
            if (fromBackup != null) {
                return fromBackup;
            }
        }
        MapOperation operation = operationProvider.createGetOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        return invokeOperation(key, operation);
    }

    private Data readBackupDataOrNull(Data key) {
        int partitionId = partitionService.getPartitionId(key);
        IPartition partition = partitionService.getPartition(partitionId, false);
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

    protected InternalCompletableFuture<Data> getAsyncInternal(Data key) {
        int partitionId = partitionService.getPartitionId(key);

        MapOperation operation = operationProvider.createGetOperation(name, key);
        try {
            long startTime = System.currentTimeMillis();
            InternalCompletableFuture<Data> future = operationService
                    .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                    .setResultDeserialized(false)
                    .invoke();

            if (statisticsEnabled) {
                future.andThen(new IncrementStatsExecutionCallback<Data>(operation, startTime));
            }

            return future;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected Data putInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        MapOperation operation = operationProvider.createPutOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        return (Data) invokeOperation(key, operation);
    }

    protected boolean tryPutInternal(Data key, Data value, long timeout, TimeUnit timeunit) {
        MapOperation operation = operationProvider.createTryPutOperation(name, key, value, getTimeInMillis(timeout, timeunit));
        return (Boolean) invokeOperation(key, operation);
    }

    protected Data putIfAbsentInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        MapOperation operation = operationProvider.createPutIfAbsentOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        return (Data) invokeOperation(key, operation);
    }

    protected void putTransientInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        MapOperation operation = operationProvider.createPutTransientOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        invokeOperation(key, operation);
    }

    private Object invokeOperation(Data key, MapOperation operation) {
        int partitionId = getNodeEngine().getPartitionService().getPartitionId(key);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            Object result;
            if (statisticsEnabled) {
                long time = System.currentTimeMillis();
                Future future = operationService
                        .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setResultDeserialized(false)
                        .invoke();
                result = future.get();
                mapServiceContext.incrementOperationStats(time, localMapStats, name, operation);
            } else {
                Future future = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setResultDeserialized(false).invoke();
                result = future.get();
            }
            return result;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected InternalCompletableFuture<Data> putAsyncInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        int partitionId = getNodeEngine().getPartitionService().getPartitionId(key);
        MapOperation operation = operationProvider.createPutOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            long startTime = System.currentTimeMillis();
            InternalCompletableFuture<Data> future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);

            if (statisticsEnabled) {
                future.andThen(new IncrementStatsExecutionCallback<Data>(operation, startTime));
            }

            return future;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected InternalCompletableFuture<Data> setAsyncInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        int partitionId = getNodeEngine().getPartitionService().getPartitionId(key);
        MapOperation operation = operationProvider.createSetOperation(name, key, value, getTimeInMillis(ttl, timeunit));
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            return operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected boolean replaceInternal(Data key, Data expect, Data update) {
        MapOperation operation = operationProvider.createReplaceIfSameOperation(name, key, expect, update);
        return (Boolean) invokeOperation(key, operation);
    }

    protected Data replaceInternal(Data key, Data value) {
        MapOperation operation = operationProvider.createReplaceOperation(name, key, value);
        return (Data) invokeOperation(key, operation);
    }

    //warning: When UpdateEvent is fired it does *NOT* contain oldValue.
    //see this: https://github.com/hazelcast/hazelcast/pull/6088#issuecomment-136025968
    protected void setInternal(Data key, Data value, long ttl, TimeUnit timeunit) {
        MapOperation operation = operationProvider.createSetOperation(name, key, value, timeunit.toMillis(ttl));
        invokeOperation(key, operation);
    }

    /**
     * Evicts a key from a map.
     *
     * @param key the key to evict
     * @return {@code true} if eviction was successful, {@code false} otherwise
     */
    protected boolean evictInternal(Data key) {
        MapOperation operation = operationProvider.createEvictOperation(name, key, false);
        return (Boolean) invokeOperation(key, operation);
    }

    protected void evictAllInternal() {
        try {
            Operation operation = operationProvider.createEvictAllOperation(name);
            BinaryOperationFactory factory = new BinaryOperationFactory(operation, getNodeEngine());
            Map<Integer, Object> resultMap = operationService.invokeOnAllPartitions(SERVICE_NAME, factory);

            int evictedCount = 0;
            for (Object object : resultMap.values()) {
                evictedCount += (Integer) object;
            }

            if (evictedCount > 0) {
                publishMapEvent(evictedCount, EntryEventType.EVICT_ALL);
            }

        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected void loadAllInternal(boolean replaceExistingValues) {
        int mapNamePartition = partitionService.getPartitionId(name);

        Operation operation = operationProvider.createLoadMapOperation(name, replaceExistingValues);
        Future loadMapFuture = operationService.invokeOnPartition(SERVICE_NAME, operation, mapNamePartition);

        try {
            loadMapFuture.get();
            waitUntilLoaded();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * Maps keys to corresponding partitions and sends operations to them.
     */
    protected void loadInternal(Iterable<Data> dataKeys, boolean replaceExistingValues) {
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

    protected <K> Iterable<Data> convertToData(Iterable<K> keys) {
        return IterableUtil.map(nullToEmpty(keys), new KeyToData<K>());
    }

    private Operation createLoadAllOperation(List<Data> keys, boolean replaceExistingValues) {
        return operationProvider.createLoadAllOperation(name, keys, replaceExistingValues);
    }

    protected Data removeInternal(Data key) {
        MapOperation operation = operationProvider.createRemoveOperation(name, key, false);
        return (Data) invokeOperation(key, operation);
    }

    protected void deleteInternal(Data key) {
        MapOperation operation = operationProvider.createDeleteOperation(name, key);
        invokeOperation(key, operation);
    }

    protected boolean removeInternal(Data key, Data value) {
        MapOperation operation = operationProvider.createRemoveIfSameOperation(name, key, value);
        return (Boolean) invokeOperation(key, operation);
    }

    protected boolean tryRemoveInternal(Data key, long timeout, TimeUnit timeunit) {
        MapOperation operation = operationProvider.createTryRemoveOperation(name, key, getTimeInMillis(timeout, timeunit));
        return (Boolean) invokeOperation(key, operation);
    }

    protected void removeAllInternal(Predicate predicate) {
        OperationFactory operation = operationProvider.createPartitionWideEntryWithPredicateOperationFactory(name,
                ENTRY_REMOVING_PROCESSOR, predicate);
        try {
            operationService.invokeOnAllPartitions(SERVICE_NAME, operation);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected InternalCompletableFuture<Data> removeAsyncInternal(Data key) {
        int partitionId = getNodeEngine().getPartitionService().getPartitionId(key);
        MapOperation operation = operationProvider.createRemoveOperation(name, key, false);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            long startTime = System.currentTimeMillis();
            InternalCompletableFuture<Data> future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);

            if (statisticsEnabled) {
                future.andThen(new IncrementStatsExecutionCallback<Data>(operation, startTime));
            }

            return future;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected boolean containsKeyInternal(Data key) {
        int partitionId = partitionService.getPartitionId(key);
        MapOperation containsKeyOperation = operationProvider.createContainsKeyOperation(name, key);
        containsKeyOperation.setThreadId(ThreadUtil.getThreadId());
        containsKeyOperation.setServiceName(SERVICE_NAME);
        try {
            Future future = operationService.invokeOnPartition(SERVICE_NAME, containsKeyOperation, partitionId);
            return (Boolean) toObject(future.get());
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public void waitUntilLoaded() {
        try {
            int mapNamePartition = partitionService.getPartitionId(name);
            // first we have to check if key-load finished - otherwise the loading on other partitions might not have started.
            // In this case we can't invoke IsPartitionLoadedOperation -> they will return "true", but it won't be correct.

            int sleepDurationMillis = INITIAL_WAIT_LOAD_SLEEP_MILLIS;
            while (true) {
                Operation op = new IsKeyLoadFinishedOperation(name);
                Future<Boolean> loadingFuture = operationService.invokeOnPartition(SERVICE_NAME, op, mapNamePartition);
                if (loadingFuture.get()) {
                    break;
                }
                // sleep with some back-off
                TimeUnit.MILLISECONDS.sleep(sleepDurationMillis);
                sleepDurationMillis = (sleepDurationMillis * 2 < MAXIMAL_WAIT_LOAD_SLEEP_MILLIS)
                        ? sleepDurationMillis * 2 : MAXIMAL_WAIT_LOAD_SLEEP_MILLIS;
            }

            OperationFactory opFactory = new IsPartitionLoadedOperationFactory(name);
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, opFactory);
            // wait for all the data to be loaded on all partitions - wait forever
            waitAllTrue(results, opFactory);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private void waitAllTrue(Map<Integer, Object> results, OperationFactory operationFactory) throws InterruptedException {
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
                results = retryPartitions(retrySet, operationFactory);
                iterator = results.entrySet().iterator();
                TimeUnit.SECONDS.sleep(1);
                retrySet.clear();
            } else {
                isFinished = true;
            }
        }
    }

    private Map<Integer, Object> retryPartitions(Collection<Integer> partitions, OperationFactory operationFactory) {
        try {
            return operationService.invokeOnPartitions(SERVICE_NAME, operationFactory, partitions);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public int size() {
        try {
            OperationFactory sizeOperationFactory = operationProvider.createMapSizeOperationFactory(name);
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, sizeOperationFactory);
            int total = 0;
            for (Object result : results.values()) {
                Integer size = toObject(result);
                total += size;
            }
            return total;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public boolean containsValueInternal(Data dataValue) {
        try {
            OperationFactory operationFactory = operationProvider.createContainsValueOperationFactory(name, dataValue);
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, operationFactory);
            for (Object result : results.values()) {
                Boolean contains = toObject(result);
                if (contains) {
                    return true;
                }
            }
            return false;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public boolean isEmpty() {
        try {
            // TODO: we don't need to wait for all futures to complete, we can stop on the first returned false
            // also there is no need to make use of IsEmptyOperation, just use size to reduce the amount of code
            IsEmptyOperationFactory factory = new IsEmptyOperationFactory(name);
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, factory);
            for (Object result : results.values()) {
                if (!(Boolean) toObject(result)) {
                    return false;
                }
            }
            return true;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected void getAllObjectInternal(List<Data> keys, List<Object> resultingKeyValuePairs) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        if (keys.isEmpty()) {
            return;
        }
        Collection<Integer> partitions = getPartitionsForKeys(keys);
        Map<Integer, Object> responses;
        try {
            OperationFactory operationFactory = operationProvider.createGetAllOperationFactory(name, keys);
            long time = System.currentTimeMillis();
            responses = operationService.invokeOnPartitions(SERVICE_NAME, operationFactory, partitions);
            for (Object response : responses.values()) {
                MapEntries entries = toObject(response);
                for (int i = 0; i < entries.size(); i++) {
                    resultingKeyValuePairs.add(toObject(entries.getKey(i)));
                    resultingKeyValuePairs.add(toObject(entries.getValue(i)));
                }
            }
            localMapStats.incrementGets(keys.size(), System.currentTimeMillis() - time);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private Collection<Integer> getPartitionsForKeys(Collection<Data> keys) {
        int partitions = partitionService.getPartitionCount();
        // TODO: is there better way to estimate the size?
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

    private boolean isPutAllUseBatching(int mapSize) {
        // we check if the feature is enabled and if the map size is bigger than a single batch per member
        return (putAllBatchSize > 0 && mapSize > putAllBatchSize * getNodeEngine().getClusterService().getSize());
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private int getPutAllInitialSize(boolean useBatching, int mapSize, int partitionCount) {
        if (mapSize == 1) {
            return 1;
        }
        if (useBatching) {
            return putAllBatchSize;
        }
        if (putAllInitialSizeFactor < 1) {
            // this is an educated guess for the initial size of the entries per partition, depending on the map size
            return (int) ceil(20f * mapSize / partitionCount / log10(mapSize));
        }
        return (int) ceil(putAllInitialSizeFactor * mapSize / partitionCount);
    }

    /**
     * This method will group all puts per partition and send a
     * {@link com.hazelcast.map.impl.operation.PutAllPartitionAwareOperationFactory} per member.
     * <p/>
     * If there are e.g. five keys for a single member, there will only be a single remote invocation
     * instead of having five remote invocations.
     * <p/>
     * There is also an optional support for batching to send smaller packages.
     * Takes care about {@code null} checks for keys and values.
     */
    @SuppressWarnings({"checkstyle:npathcomplexity", "UnnecessaryBoxing"})
    @SuppressFBWarnings(value = "DM_NUMBER_CTOR", justification = "we need a shared counter object for each member per partition")
    protected void putAllInternal(Map<?, ?> map) {
        try {
            int mapSize = map.size();
            if (mapSize == 0) {
                return;
            }

            boolean useBatching = isPutAllUseBatching(mapSize);
            int partitionCount = partitionService.getPartitionCount();
            int initialSize = getPutAllInitialSize(useBatching, mapSize, partitionCount);

            Map<Address, List<Integer>> memberPartitionsMap = partitionService.getMemberPartitionsMap();

            // init counters for batching
            MutableLong[] counterPerMember = null;
            Address[] addresses = null;
            if (useBatching) {
                counterPerMember = new MutableLong[partitionCount];
                addresses = new Address[partitionCount];
                for (Entry<Address, List<Integer>> addressListEntry : memberPartitionsMap.entrySet()) {
                    MutableLong counter = new MutableLong();
                    Address address = addressListEntry.getKey();
                    for (int partitionId : addressListEntry.getValue()) {
                        counterPerMember[partitionId] = counter;
                        addresses[partitionId] = address;
                    }
                }
            }

            // fill entriesPerPartition
            MapEntries[] entriesPerPartition = new MapEntries[partitionCount];
            for (Entry entry : map.entrySet()) {
                checkNotNull(entry.getKey(), NULL_KEY_IS_NOT_ALLOWED);
                checkNotNull(entry.getValue(), NULL_VALUE_IS_NOT_ALLOWED);

                Data keyData = toData(entry.getKey(), partitionStrategy);
                int partitionId = partitionService.getPartitionId(keyData);
                MapEntries entries = entriesPerPartition[partitionId];
                if (entries == null) {
                    entries = new MapEntries(initialSize);
                    entriesPerPartition[partitionId] = entries;
                }

                entries.add(keyData, toData(entry.getValue()));

                if (useBatching) {
                    long currentSize = ++counterPerMember[partitionId].value;
                    if (currentSize % putAllBatchSize == 0) {
                        List<Integer> partitions = memberPartitionsMap.get(addresses[partitionId]);
                        invokePutAllOperation(addresses[partitionId], partitions, entriesPerPartition);
                    }
                }
            }

            // invoke operations for entriesPerPartition
            for (Entry<Address, List<Integer>> entry : memberPartitionsMap.entrySet()) {
                invokePutAllOperation(entry.getKey(), entry.getValue(), entriesPerPartition);
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void invokePutAllOperation(Address address, List<Integer> memberPartitions, MapEntries[] entriesPerPartition)
            throws Exception {
        int size = memberPartitions.size();
        int[] partitions = new int[size];
        int index = 0;
        for (Integer partitionId : memberPartitions) {
            if (entriesPerPartition[partitionId] != null) {
                partitions[index++] = partitionId;
            }
        }
        if (index == 0) {
            return;
        }
        // trim partition array to real size
        if (index < size) {
            partitions = Arrays.copyOf(partitions, index);
            size = index;
        }

        index = 0;
        MapEntries[] entries = new MapEntries[size];
        long totalSize = 0;
        for (int partitionId : partitions) {
            int batchSize = entriesPerPartition[partitionId].size();
            assert (putAllBatchSize == 0 || batchSize <= putAllBatchSize);
            entries[index++] = entriesPerPartition[partitionId];
            totalSize += batchSize;
            entriesPerPartition[partitionId] = null;
        }
        if (totalSize == 0) {
            return;
        }

        invokePutAllOperationFactory(address, totalSize, partitions, entries);
    }

    protected void invokePutAllOperationFactory(Address address, final long size, int[] partitions, MapEntries[] entries)
            throws Exception {
        OperationFactory factory = operationProvider.createPutAllOperationFactory(name, partitions, entries);
        final long time = System.currentTimeMillis();
        operationService.invokeOnPartitions(SERVICE_NAME, factory, partitions);
        localMapStats.incrementPuts(size, System.currentTimeMillis() - time);
    }

    // TODO: add a feature to mancenter to sync cache to db completely
    public void flush() {
        try {
            MapOperation mapFlushOperation = operationProvider.createMapFlushOperation(name);
            BinaryOperationFactory operationFactory = new BinaryOperationFactory(mapFlushOperation, getNodeEngine());
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, operationFactory);

            List<Future> futures = new ArrayList<Future>();
            for (Entry<Integer, Object> entry : results.entrySet()) {
                Integer partitionId = entry.getKey();
                Long count = ((Long) entry.getValue());
                if (count != 0) {
                    Operation operation = new AwaitMapFlushOperation(name, count);
                    futures.add(operationService.invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId));
                }
            }

            for (Future future : futures) {
                future.get();
            }

        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public void clearInternal() {
        try {
            Operation clearOperation = operationProvider.createClearOperation(name);
            clearOperation.setServiceName(SERVICE_NAME);
            BinaryOperationFactory factory = new BinaryOperationFactory(clearOperation, getNodeEngine());
            Map<Integer, Object> resultMap = operationService.invokeOnAllPartitions(SERVICE_NAME, factory);

            int clearedCount = 0;
            for (Object object : resultMap.values()) {
                clearedCount += (Integer) object;
            }

            if (clearedCount > 0) {
                publishMapEvent(clearedCount, CLEAR_ALL);
            }

        } catch (Throwable t) {
            throw rethrow(t);
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
                Future future = operationService.invokeOnTarget(SERVICE_NAME, op, member.getAddress());
                future.get();
            } catch (Throwable t) {
                throw rethrow(t);
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
                Future future = operationService.invokeOnTarget(SERVICE_NAME, op, member.getAddress());
                future.get();
            } catch (Throwable t) {
                throw rethrow(t);
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
        MapOperation operation = operationProvider.createGetEntryViewOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        operation.setServiceName(SERVICE_NAME);
        try {
            Future future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
            return (EntryView) toObject(future.get());
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public Data executeOnKeyInternal(Data key, EntryProcessor entryProcessor) {
        int partitionId = partitionService.getPartitionId(key);
        MapOperation operation = operationProvider.createEntryOperation(name, key, entryProcessor);
        operation.setThreadId(ThreadUtil.getThreadId());
        validateEntryProcessorForSingleKeyProcessing(entryProcessor);
        try {
            Future future = operationService
                    .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                    .setResultDeserialized(false)
                    .invoke();
            return (Data) future.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static void validateEntryProcessorForSingleKeyProcessing(EntryProcessor entryProcessor) {
        if (entryProcessor instanceof ReadOnly) {
            EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
            if (backupProcessor != null) {
                throw new IllegalArgumentException(
                        "EntryProcessor.getBackupProcessor() should be null for a ReadOnly EntryProcessor");
            }
        }
    }

    public Map executeOnKeysInternal(Set<Data> keys, EntryProcessor entryProcessor) {
        // TODO: why are we not forwarding to executeOnKeysInternal(keys, entryProcessor, null) or some other kind of fake
        // callback? now there is a lot of code duplication
        Map<Object, Object> result = new HashMap<Object, Object>();
        Collection<Integer> partitionsForKeys = getPartitionsForKeys(keys);
        try {
            OperationFactory operationFactory = operationProvider.createMultipleEntryOperationFactory(name, keys, entryProcessor);
            Map<Integer, Object> results = operationService.invokeOnPartitions(SERVICE_NAME, operationFactory, partitionsForKeys);
            for (Object object : results.values()) {
                if (object != null) {
                    MapEntries mapEntries = (MapEntries) object;
                    mapEntries.putAllToMap(serializationService, result);
                }
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return result;
    }

    public InternalCompletableFuture<Object> executeOnKeyInternal(
            Data key, EntryProcessor entryProcessor, ExecutionCallback<Object> callback) {
        int partitionId = partitionService.getPartitionId(key);
        MapOperation operation = operationProvider.createEntryOperation(name, key, entryProcessor);
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            if (callback == null) {
                return operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
            } else {
                return operationService
                        .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setExecutionCallback(new MapExecutionCallbackAdapter(callback))
                        .invoke();
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * {@link IMap#executeOnEntries(EntryProcessor, Predicate)}
     */
    public void executeOnEntriesInternal(EntryProcessor entryProcessor, Predicate predicate, List<Data> result) {
        try {
            OperationFactory operation
                    = operationProvider.createPartitionWideEntryWithPredicateOperationFactory(name, entryProcessor, predicate);
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, operation);
            for (Object object : results.values()) {
                if (object != null) {
                    MapEntries mapEntries = (MapEntries) object;
                    for (int i = 0; i < mapEntries.size(); i++) {
                        result.add(mapEntries.getKey(i));
                        result.add(mapEntries.getValue(i));
                    }
                }
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected <T> T toObject(Object object) {
        return serializationService.toObject(object);
    }

    protected Data toData(Object object, PartitioningStrategy partitioningStrategy) {
        return serializationService.toData(object, partitioningStrategy);
    }

    public void addIndex(String attribute, boolean ordered) {
        validateIndexAttribute(attribute);
        try {
            AddIndexOperation addIndexOperation = new AddIndexOperation(name, attribute, ordered);
            operationService.invokeOnAllPartitions(SERVICE_NAME, new BinaryOperationFactory(addIndexOperation, getNodeEngine()));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public LocalMapStats getLocalMapStats() {
        if (!mapConfig.isStatisticsEnabled()) {
            return EMPTY_LOCAL_MAP_STATS;
        }
        return mapServiceContext.getLocalMapStatsProvider().createLocalMapStats(name);
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
        return mapServiceContext.getMapQueryEngine(name);
    }

    protected boolean isMapStoreEnabled() {
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        return mapStoreConfig != null && mapStoreConfig.isEnabled();
    }

    protected MapConfig getMapConfig() {
        return mapConfig;
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final String getServiceName() {
        return SERVICE_NAME;
    }

    public PartitioningStrategy getPartitionStrategy() {
        return partitionStrategy;
    }

    private class MapExecutionCallbackAdapter implements ExecutionCallback<Object> {

        private final ExecutionCallback<Object> executionCallback;

        MapExecutionCallbackAdapter(ExecutionCallback<Object> executionCallback) {
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

    private class IncrementStatsExecutionCallback<T> implements ExecutionCallback<T> {

        private final MapOperation operation;
        private final long startTime;

        IncrementStatsExecutionCallback(MapOperation operation, long startTime) {
            this.operation = operation;
            this.startTime = startTime;
        }

        @Override
        public void onResponse(T response) {
            mapServiceContext.incrementOperationStats(startTime, localMapStats, name, operation);
        }

        @Override
        public void onFailure(Throwable t) {
        }
    }

    public void setOperationProvider(MapOperationProvider operationProvider) {
        this.operationProvider = operationProvider;
    }

    public MapOperationProvider getOperationProvider() {
        return operationProvider;
    }

    public int getTotalBackupCount() {
        return mapConfig.getBackupCount() + mapConfig.getAsyncBackupCount();
    }

    @SerializableByConvention
    private class KeyToData<K> implements IFunction<K, Data> {
        public Data apply(K key) {
            return toData(key);
        }
    }
}
