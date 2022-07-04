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

package com.hazelcast.map.impl.proxy;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.internal.locksupport.LockProxySupport;
import com.hazelcast.internal.locksupport.LockSupportServiceImpl;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.IterableUtil;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.MutableLong;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.operation.AddIndexOperation;
import com.hazelcast.map.impl.operation.AddInterceptorOperationSupplier;
import com.hazelcast.map.impl.operation.AwaitMapFlushOperation;
import com.hazelcast.map.impl.operation.IsEmptyOperationFactory;
import com.hazelcast.map.impl.operation.IsKeyLoadFinishedOperation;
import com.hazelcast.map.impl.operation.IsPartitionLoadedOperationFactory;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.operation.RemoveInterceptorOperationSupplier;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryEngine;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.map.impl.query.Result;
import com.hazelcast.map.impl.query.Target;
import com.hazelcast.map.impl.query.Target.TargetMode;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndProvider;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.query.impl.predicates.TruePredicate;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.InitializingObject;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.operationservice.BinaryOperationFactory;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.core.EntryEventType.CLEAR_ALL;
import static com.hazelcast.internal.util.CollectionUtil.asIntegerList;
import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;
import static com.hazelcast.internal.util.IterableUtil.nullToEmpty;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.MapUtil.toIntSize;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static com.hazelcast.internal.util.ThreadUtil.getThreadId;
import static com.hazelcast.internal.util.TimeUtil.timeInMsOrOneIfResultIsZero;
import static com.hazelcast.map.impl.EntryRemovingProcessor.ENTRY_REMOVING_PROCESSOR;
import static com.hazelcast.map.impl.MapOperationStatsUpdater.incrementOperationStats;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.query.Target.createPartitionTarget;
import static com.hazelcast.query.Predicates.alwaysFalse;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static java.lang.Math.ceil;
import static java.lang.Math.log10;
import static java.lang.Math.min;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

abstract class MapProxySupport<K, V>
        extends AbstractDistributedObject<MapService>
        implements IMap<K, V>, InitializingObject {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_KEYS_ARE_NOT_ALLOWED = "Null keys collection is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    protected static final String NULL_PREDICATE_IS_NOT_ALLOWED = "Predicate should not be null!";
    protected static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";
    protected static final String NULL_AGGREGATOR_IS_NOT_ALLOWED = "Aggregator should not be null!";
    protected static final String NULL_PROJECTION_IS_NOT_ALLOWED = "Projection should not be null!";
    protected static final String NULL_TTL_UNIT_IS_NOT_ALLOWED = "Null ttlUnit is not allowed!";
    protected static final String NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED = "Null maxIdleUnit is not allowed!";
    protected static final String NULL_TIMEUNIT_IS_NOT_ALLOWED = "Null timeunit is not allowed!";
    protected static final String NULL_BIFUNCTION_IS_NOT_ALLOWED = "Null BiFunction is not allowed!";
    protected static final String NULL_FUNCTION_IS_NOT_ALLOWED = "Null Function is not allowed!";
    protected static final String NULL_CONSUMER_IS_NOT_ALLOWED = "Null Consumer is not allowed!";

    private static final int INITIAL_WAIT_LOAD_SLEEP_MILLIS = 10;
    private static final int MAXIMAL_WAIT_LOAD_SLEEP_MILLIS = 1000;
    /**
     * Retry count when an interceptor registration/de-registration operation fails.
     */
    private static final int MAX_RETRIES = 100;

    /**
     * Defines the batch size for operations of {@link IMap#putAll(Map)} and {@link IMap#setAll(Map)} calls.
     * <p>
     * A value of {@code 0} disables the batching and will send a single operation per member with all map entries.
     * <p>
     * If you set this value too high, you may ran into OOME or blocked network pipelines due to huge operations.
     * If you set this value too low, you will lower the performance of the putAll() operation.
     */
    private static final HazelcastProperty MAP_PUT_ALL_BATCH_SIZE
            = new HazelcastProperty("hazelcast.map.put.all.batch.size", 0);

    /**
     * Defines the initial size of entry arrays per partition for {@link IMap#putAll(Map)} and {@link IMap#setAll(Map)} calls.
     * <p>
     * {@link IMap#putAll(Map)} / {@link IMap#setAll(Map)} splits up the entries of the user input map per partition,
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
        this.lockSupport = new LockProxySupport(MapService.getObjectNamespace(name),
                LockSupportServiceImpl.getMaxLeaseTimeInMillis(properties));
        this.operationProvider = mapServiceContext.getMapOperationProvider(name);
        this.operationService = nodeEngine.getOperationService();
        this.serializationService = nodeEngine.getSerializationService();
        this.thisAddress = nodeEngine.getClusterService().getThisAddress();
        this.statisticsEnabled = mapConfig.isStatisticsEnabled();

        this.putAllBatchSize = properties.getInteger(MAP_PUT_ALL_BATCH_SIZE);
        this.putAllInitialSizeFactor = properties.getFloat(MAP_PUT_ALL_INITIAL_SIZE_FACTOR);
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final String getServiceName() {
        return SERVICE_NAME;
    }

    public MapConfig getMapConfig() {
        return mapConfig;
    }

    @Override
    public void initialize() {
        initializeListeners();
        initializeIndexes();
        initializeMapStoreLoad();
    }

    private void initializeListeners() {
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

    private void initializeIndexes() {
        for (IndexConfig index : mapConfig.getIndexConfigs()) {
            addIndex(index);
        }
    }

    private void initializeMapStoreLoad() {
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        if (mapStoreConfig != null && mapStoreConfig.isEnabled()) {
            MapStoreConfig.InitialLoadMode initialLoadMode = mapStoreConfig.getInitialLoadMode();
            if (MapStoreConfig.InitialLoadMode.EAGER.equals(initialLoadMode)) {
                waitUntilLoaded();
            }
        }
    }

    public PartitioningStrategy getPartitionStrategy() {
        return partitionStrategy;
    }

    public MapOperationProvider getOperationProvider() {
        return operationProvider;
    }

    public void setOperationProvider(MapOperationProvider operationProvider) {
        this.operationProvider = operationProvider;
    }

    public int getTotalBackupCount() {
        return mapConfig.getBackupCount() + mapConfig.getAsyncBackupCount();
    }

    protected QueryEngine getMapQueryEngine() {
        return mapServiceContext.getQueryEngine(name);
    }

    protected boolean isMapStoreEnabled() {
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        return mapStoreConfig != null && mapStoreConfig.isEnabled();
    }

    protected Object getInternal(Object key) {
        // TODO: action for read-backup true is not well tested
        Data keyData = toDataWithStrategy(key);
        if (mapConfig.isReadBackupData()) {
            Object fromBackup = readBackupDataOrNull(keyData);
            if (fromBackup != null) {
                return fromBackup;
            }
        }
        MapOperation operation = operationProvider.createGetOperation(name, keyData);
        operation.setThreadId(getThreadId());
        return invokeOperation(keyData, operation);
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

    protected InternalCompletableFuture<Data> getAsyncInternal(Object key) {
        Data keyData = toDataWithStrategy(key);
        int partitionId = partitionService.getPartitionId(keyData);

        MapOperation operation = operationProvider.createGetOperation(name, keyData);
        try {
            long startTimeNanos = Timer.nanos();
            InvocationFuture<Data> future = operationService
                    .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                    .setResultDeserialized(false)
                    .setAsync()
                    .invoke();

            if (statisticsEnabled) {
                future.whenCompleteAsync(new IncrementStatsExecutionCallback<>(operation, startTimeNanos), CALLER_RUNS);
            }

            return future;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected Data putInternal(Object key, Data valueData,
                               long ttl, TimeUnit ttlUnit,
                               long maxIdle, TimeUnit maxIdleUnit) {

        Data keyData = toDataWithStrategy(key);
        MapOperation operation = newPutOperation(keyData, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
        return (Data) invokeOperation(keyData, operation);
    }

    private MapOperation newPutOperation(Data keyData, Data valueData,
                                         long ttl, TimeUnit timeunit,
                                         long maxIdle, TimeUnit maxIdleUnit) {
        return operationProvider.createPutOperation(name, keyData, valueData,
                timeInMsOrOneIfResultIsZero(ttl, timeunit),
                timeInMsOrOneIfResultIsZero(maxIdle, maxIdleUnit));
    }

    protected boolean tryPutInternal(Object key, Data value, long timeout, TimeUnit timeunit) {
        Data keyData = toDataWithStrategy(key);
        long timeInMillis = timeInMsOrOneIfResultIsZero(timeout, timeunit);
        MapOperation operation = operationProvider.createTryPutOperation(name, keyData, value, timeInMillis);
        return (Boolean) invokeOperation(keyData, operation);
    }

    protected Data putIfAbsentInternal(Object key, Data value,
                                       long ttl, TimeUnit ttlUnit,
                                       long maxIdle, TimeUnit maxIdleUnit) {

        Data keyData = toDataWithStrategy(key);
        MapOperation operation = newPutIfAbsentOperation(keyData, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
        return (Data) invokeOperation(keyData, operation);
    }

    private MapOperation newPutIfAbsentOperation(Data keyData, Data valueData,
                                                 long ttl, TimeUnit timeunit,
                                                 long maxIdle, TimeUnit maxIdleUnit) {
        return operationProvider.createPutIfAbsentOperation(name, keyData, valueData,
                timeInMsOrOneIfResultIsZero(ttl, timeunit),
                timeInMsOrOneIfResultIsZero(maxIdle, maxIdleUnit));
    }

    protected void putTransientInternal(Object key, Data value,
                                        long ttl, TimeUnit ttlUnit,
                                        long maxIdle, TimeUnit maxIdleUnit) {
        Data keyData = toDataWithStrategy(key);
        MapOperation operation = newPutTransientOperation(keyData, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
        invokeOperation(keyData, operation);
    }

    private MapOperation newPutTransientOperation(Data keyData, Data valueData,
                                                  long ttl, TimeUnit timeunit,
                                                  long maxIdle, TimeUnit maxIdleUnit) {
        return operationProvider.createPutTransientOperation(name, keyData, valueData,
                timeInMsOrOneIfResultIsZero(ttl, timeunit),
                timeInMsOrOneIfResultIsZero(maxIdle, maxIdleUnit));
    }

    private Object invokeOperation(Data key, MapOperation operation) {
        int partitionId = partitionService.getPartitionId(key);
        operation.setThreadId(getThreadId());
        try {
            Object result;
            if (statisticsEnabled) {
                long startTimeNanos = Timer.nanos();
                Future future = operationService
                        .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setResultDeserialized(false)
                        .invoke();
                result = future.get();
                incrementOperationStats(operation, localMapStats, startTimeNanos);
            } else {
                Future future = operationService
                        .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                        .setResultDeserialized(false)
                        .invoke();
                result = future.get();
            }
            return result;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected InternalCompletableFuture<Data> putAsyncInternal(Object key, Data valueData,
                                                               long ttl, TimeUnit ttlUnit,
                                                               long maxIdle, TimeUnit maxIdleUnit) {
        Data keyData = toDataWithStrategy(key);
        int partitionId = partitionService.getPartitionId(keyData);
        MapOperation operation = newPutOperation(keyData, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
        operation.setThreadId(getThreadId());
        try {
            long startTimeNanos = Timer.nanos();
            InvocationFuture<Data> future = operationService.invokeOnPartitionAsync(SERVICE_NAME, operation, partitionId);

            if (statisticsEnabled) {
                future.whenCompleteAsync(new IncrementStatsExecutionCallback<>(operation, startTimeNanos), CALLER_RUNS);
            }
            return future;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected InternalCompletableFuture<Data> putIfAbsentAsyncInternal(Object key, Data value,
                                                                       long ttl, TimeUnit ttlUnit,
                                                                       long maxIdle, TimeUnit maxIdleUnit) {
        Data keyData = toDataWithStrategy(key);
        int partitionId = partitionService.getPartitionId(key);
        MapOperation operation = newPutIfAbsentOperation(keyData, value, ttl, ttlUnit, maxIdle, maxIdleUnit);
        operation.setThreadId(getThreadId());
        try {
            long startTimeNanos = Timer.nanos();
            InvocationFuture<Data> future = operationService.invokeOnPartitionAsync(SERVICE_NAME, operation, partitionId);
            if (statisticsEnabled) {
                future.whenCompleteAsync(new IncrementStatsExecutionCallback<>(operation, startTimeNanos), CALLER_RUNS);
            }
            return future;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected InternalCompletableFuture<Data> setAsyncInternal(Object key, Data valueData, long ttl, TimeUnit timeunit,
                                                               long maxIdle, TimeUnit maxIdleUnit) {
        Data keyData = toDataWithStrategy(key);
        int partitionId = partitionService.getPartitionId(keyData);

        MapOperation operation = newSetOperation(keyData, valueData, ttl, timeunit, maxIdle, maxIdleUnit);
        operation.setThreadId(getThreadId());

        try {
            final InvocationFuture<Data> result;
            if (statisticsEnabled) {
                long startTimeNanos = Timer.nanos();
                result = operationService
                        .invokeOnPartitionAsync(SERVICE_NAME, operation, partitionId);
                result.whenCompleteAsync(new IncrementStatsExecutionCallback<>(operation, startTimeNanos), CALLER_RUNS);
            } else {
                result = operationService
                        .invokeOnPartitionAsync(SERVICE_NAME, operation, partitionId);
            }
            return result;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected boolean replaceInternal(Object key, Data expect, Data update) {
        Data keyData = toDataWithStrategy(key);
        MapOperation operation = operationProvider.createReplaceIfSameOperation(name, keyData, expect, update);
        return (Boolean) invokeOperation(keyData, operation);
    }

    protected Data replaceInternal(Object key, Data value) {
        Data keyData = toDataWithStrategy(key);
        MapOperation operation = operationProvider.createReplaceOperation(name, keyData, value);
        return (Data) invokeOperation(keyData, operation);
    }

    // WARNING: when UpdateEvent is fired it does *NOT* contain the oldValue
    // see this: https://github.com/hazelcast/hazelcast/pull/6088#issuecomment-136025968
    protected void setInternal(Object key, Data valueData, long ttl, TimeUnit timeunit, long maxIdle, TimeUnit maxIdleUnit) {
        Data keyData = toDataWithStrategy(key);
        MapOperation operation = newSetOperation(keyData, valueData, ttl, timeunit, maxIdle, maxIdleUnit);
        invokeOperation(keyData, operation);
    }

    private MapOperation newSetOperation(Data keyData, Data valueData,
                                         long ttl, TimeUnit timeunit,
                                         long maxIdle, TimeUnit maxIdleUnit) {
        return operationProvider.createSetOperation(name, keyData, valueData,
                timeInMsOrOneIfResultIsZero(ttl, timeunit),
                timeInMsOrOneIfResultIsZero(maxIdle, maxIdleUnit));
    }

    /**
     * Evicts a key from a map.
     *
     * @param key the key to evict
     * @return {@code true} if eviction was successful, {@code false} otherwise
     */
    protected boolean evictInternal(Object key) {
        Data keyData = toDataWithStrategy(key);
        MapOperation operation = operationProvider.createEvictOperation(name, keyData, false);
        return (Boolean) invokeOperation(keyData, operation);
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
    protected void loadInternal(Set<K> keys, Iterable<Data> dataKeys, boolean replaceExistingValues) {
        if (dataKeys == null) {
            dataKeys = convertToData(keys);
        }
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

    protected Iterable<Data> convertToData(Iterable<K> keys) {
        return IterableUtil.map(nullToEmpty(keys), new KeyToData());
    }

    private Operation createLoadAllOperation(List<Data> keys, boolean replaceExistingValues) {
        return operationProvider.createLoadAllOperation(name, keys, replaceExistingValues);
    }

    protected Data removeInternal(Object key) {
        Data keyData = toDataWithStrategy(key);
        MapOperation operation = operationProvider.createRemoveOperation(name, keyData);
        return (Data) invokeOperation(keyData, operation);
    }

    protected void deleteInternal(Object key) {
        Data keyData = toDataWithStrategy(key);
        MapOperation operation = operationProvider.createDeleteOperation(name, keyData, false);
        invokeOperation(keyData, operation);
    }

    protected boolean removeInternal(Object key, Data value) {
        Data keyData = toDataWithStrategy(key);
        MapOperation operation = operationProvider.createRemoveIfSameOperation(name, keyData, value);
        return (Boolean) invokeOperation(keyData, operation);
    }

    protected boolean tryRemoveInternal(Object key, long timeout, TimeUnit timeunit) {
        Data keyData = toDataWithStrategy(key);
        MapOperation operation = operationProvider.createTryRemoveOperation(name, keyData,
                timeInMsOrOneIfResultIsZero(timeout, timeunit));
        return (Boolean) invokeOperation(keyData, operation);
    }

    protected void removeAllInternal(Predicate predicate) {
        try {
            if (predicate instanceof PartitionPredicate) {
                PartitionPredicate partitionPredicate = (PartitionPredicate) predicate;
                OperationFactory operation = operationProvider
                        .createPartitionWideEntryWithPredicateOperationFactory(name, ENTRY_REMOVING_PROCESSOR,
                                partitionPredicate.getTarget());
                Data partitionKey = toDataWithStrategy(partitionPredicate.getPartitionKey());
                int partitionId = partitionService.getPartitionId(partitionKey);

                // invokeOnPartitions is used intentionally here, instead of invokeOnPartition, since
                // the later one doesn't support PartitionAwareOperationFactory, which we need to use
                // to speed up the removal operation using global indexes
                // (see PartitionWideEntryWithPredicateOperationFactory.createFactoryOnRunner).
                operationService.invokeOnPartitions(SERVICE_NAME, operation, singletonList(partitionId));
            } else {
                OperationFactory operation = operationProvider
                        .createPartitionWideEntryWithPredicateOperationFactory(name, ENTRY_REMOVING_PROCESSOR, predicate);
                operationService.invokeOnAllPartitions(SERVICE_NAME, operation);
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected boolean setTtlInternal(Object key, long ttl, TimeUnit timeUnit) {
        long ttlInMillis = timeUnit.toMillis(ttl);
        Data keyData = serializationService.toData(key);
        MapOperation operation = operationProvider.createSetTtlOperation(name, keyData, ttlInMillis);
        return (Boolean) invokeOperation(keyData, operation);
    }

    protected InternalCompletableFuture<Data> removeAsyncInternal(Object key) {
        Data keyData = toDataWithStrategy(key);
        int partitionId = partitionService.getPartitionId(keyData);
        MapOperation operation = operationProvider.createRemoveOperation(name, keyData);
        operation.setThreadId(getThreadId());
        try {
            long startTimeNanos = Timer.nanos();
            InvocationFuture<Data> future = operationService.invokeOnPartitionAsync(SERVICE_NAME, operation, partitionId);

            if (statisticsEnabled) {
                future.whenCompleteAsync(new IncrementStatsExecutionCallback<>(operation, startTimeNanos), CALLER_RUNS);
            }

            return future;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected boolean containsKeyInternal(Object key) {
        Data keyData = toDataWithStrategy(key);
        int partitionId = partitionService.getPartitionId(keyData);
        MapOperation containsKeyOperation = operationProvider.createContainsKeyOperation(name, keyData);
        containsKeyOperation.setThreadId(getThreadId());
        containsKeyOperation.setServiceName(SERVICE_NAME);
        try {
            Future future = operationService.invokeOnPartition(SERVICE_NAME, containsKeyOperation, partitionId);
            Object object = future.get();
            incrementOtherOperationsStat();
            return (Boolean) toObject(object);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public void waitUntilLoaded() {
        try {
            int mapNamesPartitionId = partitionService.getPartitionId(name);
            // first we have to check if key-load finished - otherwise
            // the loading on other partitions might not have started.
            // In this case we can't invoke IsPartitionLoadedOperation
            // -> they will return "true", but it won't be correct

            int sleepDurationMillis = INITIAL_WAIT_LOAD_SLEEP_MILLIS;
            while (true) {
                Operation op = new IsKeyLoadFinishedOperation(name);
                Future<Boolean> loadingFuture = operationService.invokeOnPartition(SERVICE_NAME, op, mapNamesPartitionId);
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
        PartitionIdSet retrySet = new PartitionIdSet(partitionService.getPartitionCount());
        while (!isFinished) {
            while (iterator.hasNext()) {
                Entry<Integer, Object> entry = iterator.next();
                if (Boolean.TRUE.equals(entry.getValue())) {
                    iterator.remove();
                } else {
                    retrySet.add(entry.getKey());
                }
            }
            if (!retrySet.isEmpty()) {
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

    @Override
    public int size() {
        try {
            OperationFactory sizeOperationFactory = operationProvider.createMapSizeOperationFactory(name);
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, sizeOperationFactory);
            incrementOtherOperationsStat();
            long total = 0;
            for (Object result : results.values()) {
                Integer size = toObject(result);
                total += size;
            }
            return toIntSize(total);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public boolean containsValueInternal(Data dataValue) {
        try {
            OperationFactory operationFactory = operationProvider.createContainsValueOperationFactory(name, dataValue);
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, operationFactory);
            incrementOtherOperationsStat();
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

    @Override
    public boolean isEmpty() {
        try {
            // TODO: we don't need to wait for all futures to complete, we can stop on the first returned false
            // also there is no need to make use of IsEmptyOperation, just use size to reduce the amount of code
            IsEmptyOperationFactory factory = new IsEmptyOperationFactory(name);
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, factory);
            incrementOtherOperationsStat();
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

    protected void incrementOtherOperationsStat() {
        if (statisticsEnabled) {
            localMapStats.incrementOtherOperations();
        }
    }

    protected void getAllInternal(Set<K> keys, List<Data> dataKeys, List<Object> resultingKeyValuePairs) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        if (dataKeys.isEmpty()) {
            toDataCollectionWithNonNullKeyValidation(keys, dataKeys);
        }
        Collection<Integer> partitions = getPartitionsForKeys(dataKeys);
        Map<Integer, Object> responses;
        try {
            OperationFactory operationFactory = operationProvider.createGetAllOperationFactory(name, dataKeys);
            long startTimeNanos = Timer.nanos();

            responses = operationService.invokeOnPartitions(SERVICE_NAME, operationFactory, partitions);
            for (Object response : responses.values()) {
                MapEntries entries = toObject(response);
                for (int i = 0; i < entries.size(); i++) {
                    resultingKeyValuePairs.add(entries.getKey(i));
                    resultingKeyValuePairs.add(entries.getValue(i));
                }
            }
            localMapStats.incrementGetLatencyNanos(dataKeys.size(), Timer.nanosElapsed(startTimeNanos));
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private Collection<Integer> getPartitionsForKeys(Collection<Data> keys) {
        int partitions = partitionService.getPartitionCount();
        // TODO: is there better way to estimate the size?
        int capacity = min(partitions, keys.size());
        Set<Integer> partitionIds = createHashSet(capacity);

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

        Map<Integer, List<Data>> idToKeys = new HashMap<>();
        for (Data key : keys) {
            int partitionId = partitionService.getPartitionId(key);
            List<Data> keyList = idToKeys.computeIfAbsent(partitionId, k -> new ArrayList<>());
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
     * This method will group all entries per partition and send one operation
     * per member. If there are e.g. five keys for a single member, even if
     * they are from different partitions, there will only be a single remote
     * invocation instead of five.
     * <p>
     * There is also an optional support for batching to send smaller packages.
     * Takes care about {@code null} checks for keys and values.
     *
     * @param future iff not-null, execute asynchronously by completing this future.
     *               Batching is not supported in async mode
     */
    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    protected void putAllInternal(Map<? extends K, ? extends V> map,
                                  @Nullable InternalCompletableFuture<Void> future,
                                  boolean triggerMapLoader) {
        try {
            int mapSize = map.size();
            if (mapSize == 0) {
                if (future != null) {
                    future.complete(null);
                }
                return;
            }

            boolean useBatching = future == null && isPutAllUseBatching(mapSize);
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

                Data keyData = toDataWithStrategy(entry.getKey());
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
                        invokePutAllOperation(addresses[partitionId], partitions, entriesPerPartition, triggerMapLoader)
                                .get();
                    }
                }
            }

            // invoke operations for entriesPerPartition
            AtomicInteger counter = new AtomicInteger(memberPartitionsMap.size());
            InternalCompletableFuture<Void> resultFuture =
                    future != null ? future : new InternalCompletableFuture<>();
            BiConsumer<Void, Throwable> callback = (response, t) -> {
                if (t != null) {
                    resultFuture.completeExceptionally(t);
                }
                if (counter.decrementAndGet() == 0) {
                    try {
                        // don't ignore errors here, see https://github.com/hazelcast/hazelcast-jet/issues/3046
                        finalizePutAll(map);
                    } catch (Throwable e) {
                        resultFuture.completeExceptionally(e);
                        return;
                    }
                    if (!resultFuture.isDone()) {
                        resultFuture.complete(null);
                    }
                }
            };
            for (Entry<Address, List<Integer>> entry : memberPartitionsMap.entrySet()) {
                invokePutAllOperation(entry.getKey(), entry.getValue(), entriesPerPartition, triggerMapLoader)
                        .whenCompleteAsync(callback);
            }
            // if executing in sync mode, block for the responses
            if (future == null) {
                resultFuture.get();
            }
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    @Nonnull
    private InternalCompletableFuture<Void> invokePutAllOperation(
            Address address,
            List<Integer> memberPartitions,
            MapEntries[] entriesPerPartition,
            boolean triggerMapLoader
    ) {
        int size = memberPartitions.size();
        int[] partitions = new int[size];
        int index = 0;
        for (Integer partitionId : memberPartitions) {
            if (entriesPerPartition[partitionId] != null) {
                partitions[index++] = partitionId;
            }
        }
        if (index == 0) {
            return newCompletedFuture(null);
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
            return newCompletedFuture(null);
        }

        OperationFactory factory = operationProvider.createPutAllOperationFactory(name, partitions, entries, triggerMapLoader);
        long startTimeNanos = Timer.nanos();
        CompletableFuture<Map<Integer, Object>> future =
                operationService.invokeOnPartitionsAsync(SERVICE_NAME, factory, singletonMap(address, asIntegerList(partitions)));
        InternalCompletableFuture<Void> resultFuture = new InternalCompletableFuture<>();
        long finalTotalSize = totalSize;
        future.whenCompleteAsync((response, t) -> {
            putAllVisitSerializedKeys(entries);
            if (t == null) {
                localMapStats.incrementPutLatencyNanos(finalTotalSize, Timer.nanosElapsed(startTimeNanos));
                resultFuture.complete(null);
            } else {
                resultFuture.completeExceptionally(t);
            }
        }, CALLER_RUNS);
        return resultFuture;
    }

    protected void putAllVisitSerializedKeys(MapEntries[] entries) {
    }

    protected void finalizePutAll(Map<?, ?> map) {
    }

    @Override
    public void flush() {
        // TODO: add a feature to Management Center to sync cache to db completely
        try {
            MapOperation mapFlushOperation = operationProvider.createMapFlushOperation(name);
            BinaryOperationFactory operationFactory = new BinaryOperationFactory(mapFlushOperation, getNodeEngine());
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, operationFactory);

            List<Future> futures = new ArrayList<>();
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

            incrementOtherOperationsStat();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public String addMapInterceptorInternal(MapInterceptor interceptor) {
        String id = mapServiceContext.generateInterceptorId(name, interceptor);
        syncInvokeOnAllMembers(new AddInterceptorOperationSupplier(name, id, interceptor));
        return id;
    }

    protected boolean removeMapInterceptorInternal(String id) {
        return syncInvokeOnAllMembers(new RemoveInterceptorOperationSupplier(name, id));
    }

    private <T> T syncInvokeOnAllMembers(Supplier<Operation> operationSupplier) {
        CompletableFuture<Object> future = invokeOnStableClusterSerial(getNodeEngine(),
                operationSupplier, MAX_RETRIES);
        try {
            return (T) future.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public UUID addLocalEntryListenerInternal(Object listener) {
        return addLocalEntryListenerInternal(listener, TruePredicate.INSTANCE, null, true);
    }

    public UUID addLocalEntryListenerInternal(Object listener, Predicate predicate, Data key, boolean includeValue) {
        EventFilter eventFilter = new QueryEventFilter(key, predicate, includeValue);
        return mapServiceContext.addLocalEventListener(listener, eventFilter, name);
    }

    protected UUID addEntryListenerInternal(Object listener, Data key, boolean includeValue) {
        EventFilter eventFilter = new EntryEventFilter(key, includeValue);
        return mapServiceContext.addEventListener(listener, eventFilter, name);
    }

    protected UUID addEntryListenerInternal(Object listener,
                                            Predicate predicate,
                                            @Nullable Data key,
                                            boolean includeValue) {
        EventFilter eventFilter = new QueryEventFilter(key, predicate, includeValue);
        return mapServiceContext.addEventListener(listener, eventFilter, name);
    }

    protected boolean removeEntryListenerInternal(UUID id) {
        return mapServiceContext.removeEventListener(name, id);
    }

    protected UUID addPartitionLostListenerInternal(MapPartitionLostListener listener) {
        return mapServiceContext.addPartitionLostListener(listener, name);
    }

    protected boolean removePartitionLostListenerInternal(UUID id) {
        return mapServiceContext.removePartitionLostListener(name, id);
    }

    protected EntryView getEntryViewInternal(Data key) {
        int partitionId = partitionService.getPartitionId(key);
        MapOperation operation = operationProvider.createGetEntryViewOperation(name, key);
        operation.setThreadId(getThreadId());
        operation.setServiceName(SERVICE_NAME);
        try {
            Future future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
            return (EntryView) toObject(future.get());
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public InternalCompletableFuture<Data> executeOnKeyInternal(Object key, EntryProcessor entryProcessor) {
        Data keyData = toDataWithStrategy(key);
        int partitionId = partitionService.getPartitionId(keyData);
        MapOperation operation = operationProvider.createEntryOperation(name, keyData, entryProcessor);
        operation.setThreadId(getThreadId());
        validateEntryProcessorForSingleKeyProcessing(entryProcessor);
        return operationService
                .createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                .setResultDeserialized(false)
                .invoke();
    }

    private static void validateEntryProcessorForSingleKeyProcessing(EntryProcessor entryProcessor) {
        if (entryProcessor instanceof ReadOnly) {
            EntryProcessor backupProcessor = entryProcessor.getBackupProcessor();
            if (backupProcessor != null) {
                throw new IllegalArgumentException(
                        "EntryProcessor.getBackupProcessor() should be null for a ReadOnly EntryProcessor");
            }
        }
    }

    public <R> InternalCompletableFuture<Map<K, R>> submitToKeysInternal(Set<K> keys, Set<Data> dataKeys,
                                                                         EntryProcessor<K, V, R> entryProcessor) {
        if (dataKeys.isEmpty()) {
            toDataCollectionWithNonNullKeyValidation(keys, dataKeys);
        }
        Collection<Integer> partitionsForKeys = getPartitionsForKeys(dataKeys);
        OperationFactory operationFactory = operationProvider.createMultipleEntryOperationFactory(name, dataKeys,
                entryProcessor);

        final InternalCompletableFuture resultFuture = new InternalCompletableFuture();
        operationService.invokeOnPartitionsAsync(SERVICE_NAME, operationFactory, partitionsForKeys)
                .whenCompleteAsync((response, throwable) -> {
                    if (throwable == null) {
                        Map<K, Object> result = null;
                        try {
                            result = createHashMap(response.size());
                            for (Object object : response.values()) {
                                MapEntries mapEntries = (MapEntries) object;
                                mapEntries.putAllToMap(serializationService, result);
                            }
                        } catch (Throwable e) {
                            resultFuture.completeExceptionally(e);
                        }
                        resultFuture.complete(result);
                    } else {
                        resultFuture.completeExceptionally(throwable);
                    }
                });
        return resultFuture;
    }

    /**
     * {@link IMap#executeOnEntries(EntryProcessor, Predicate)}
     */
    public void executeOnEntriesInternal(EntryProcessor entryProcessor, Predicate predicate, List<Data> result) {
        try {
            Map<Integer, Object> results;
            if (predicate instanceof PartitionPredicate) {
                PartitionPredicate partitionPredicate = (PartitionPredicate) predicate;
                Data key = toData(partitionPredicate.getPartitionKey());
                int partitionId = partitionService.getPartitionId(key);
                handleHazelcastInstanceAwareParams(partitionPredicate.getTarget());

                OperationFactory operation = operationProvider.createPartitionWideEntryWithPredicateOperationFactory(
                        name, entryProcessor, partitionPredicate.getTarget());
                results = operationService.invokeOnPartitions(SERVICE_NAME, operation, singletonList(partitionId));
            } else {
                OperationFactory operation = operationProvider.createPartitionWideEntryWithPredicateOperationFactory(
                        name, entryProcessor, predicate);
                results = operationService.invokeOnAllPartitions(SERVICE_NAME, operation);
            }
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

    protected Data toDataWithStrategy(Object object) {
        return serializationService.toData(object, partitionStrategy);
    }

    protected Data toData(Object object, PartitioningStrategy partitioningStrategy) {
        return serializationService.toData(object, partitioningStrategy);
    }

    @Override
    public void addIndex(IndexConfig indexConfig) {
        checkNotNull(indexConfig, "Index config cannot be null.");
        checkFalse(isNativeMemoryAndBitmapIndexingEnabled(indexConfig.getType()),
                "BITMAP indexes are not supported by NATIVE storage");

        IndexConfig indexConfig0 = IndexUtils.validateAndNormalize(name, indexConfig);

        try {
            AddIndexOperation addIndexOperation = new AddIndexOperation(name, indexConfig0);

            operationService.invokeOnAllPartitions(SERVICE_NAME,
                    new BinaryOperationFactory(addIndexOperation, getNodeEngine()));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected boolean isNativeMemoryAndBitmapIndexingEnabled(IndexType indexType) {
        InMemoryFormat mapStoreConfig = mapConfig.getInMemoryFormat();
        return mapStoreConfig == InMemoryFormat.NATIVE && indexType == IndexType.BITMAP;
    }

    @Override
    public LocalMapStats getLocalMapStats() {
        return mapServiceContext.getLocalMapStatsProvider().createLocalMapStats(name);
    }

    @Override
    protected boolean preDestroy() {
        try {
            QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
            SubscriberContext subscriberContext = queryCacheContext.getSubscriberContext();
            QueryCacheEndToEndProvider provider = subscriberContext.getEndToEndQueryCacheProvider();
            provider.destroyAllQueryCaches(name);
        } finally {
            super.preDestroy();
        }

        return true;
    }

    protected void toDataCollectionWithNonNullKeyValidation(Set<K> keys, Collection<Data> dataKeys) {
        for (K key : keys) {
            checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
            dataKeys.add(toDataWithStrategy(key));
        }
    }

    private void publishMapEvent(int numberOfAffectedEntries, EntryEventType eventType) {
        MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        mapEventPublisher.publishMapEvent(thisAddress, name, eventType, numberOfAffectedEntries);
    }

    protected <T extends Result> T executeQueryInternal(Predicate predicate, IterationType iterationType, Target target) {
        return executeQueryInternal(predicate, null, null, iterationType, target);
    }

    protected <T extends Result> T executeQueryInternal(Predicate predicate, Aggregator aggregator, Projection projection,
                                                        IterationType iterationType, Target target) {
        QueryEngine queryEngine = getMapQueryEngine();
        final Predicate userPredicate;

        if (predicate instanceof PartitionPredicate) {
            PartitionPredicate partitionPredicate = (PartitionPredicate) predicate;
            Data key = toData(partitionPredicate.getPartitionKey());
            int partitionId = partitionService.getPartitionId(key);
            if (target.mode() == TargetMode.LOCAL_NODE && !partitionService.isPartitionOwner(partitionId)
                    || target.mode() == TargetMode.PARTITION_OWNER && !target.partitions().contains(partitionId)
            ) {
                userPredicate = alwaysFalse();
            } else {
                target = createPartitionTarget(new PartitionIdSet(partitionService.getPartitionCount(), partitionId));
                userPredicate = partitionPredicate.getTarget();
            }
        } else {
            userPredicate = predicate;
        }
        handleHazelcastInstanceAwareParams(userPredicate);

        Query query = Query.of()
                .mapName(getName())
                .predicate(userPredicate)
                .iterationType(iterationType)
                .aggregator(aggregator)
                .projection(projection)
                .build();
        return queryEngine.execute(query, target);
    }

    protected void handleHazelcastInstanceAwareParams(Object... objects) {
        for (Object object : objects) {
            if (object instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) object).setHazelcastInstance(getNodeEngine().getHazelcastInstance());
            }
        }
    }

    private class IncrementStatsExecutionCallback<T> implements BiConsumer<T, Throwable> {

        private final MapOperation operation;
        private final long startTime;

        IncrementStatsExecutionCallback(MapOperation operation, long startTime) {
            this.operation = operation;
            this.startTime = startTime;
        }

        @Override
        public void accept(T t, Throwable throwable) {
            if (throwable == null) {
                incrementOperationStats(operation, localMapStats, startTime);
            }
        }
    }

    private class KeyToData implements Function<K, Data> {

        @Override
        public Data apply(K key) {
            return toDataWithStrategy(key);
        }
    }

}
