/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.internal.eviction.ExpirationManager;
import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.internal.util.InvocationUtil;
import com.hazelcast.internal.util.LocalRetryableExecution;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.internal.util.comparators.ValueComparatorUtil;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.event.MapEventPublisherImpl;
import com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask;
import com.hazelcast.map.impl.journal.MapEventJournal;
import com.hazelcast.map.impl.journal.RingbufferMapEventJournalImpl;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.writebehind.NodeWideUsedCapacityCounter;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.operation.MapOperationProviders;
import com.hazelcast.map.impl.operation.MapPartitionDestroyOperation;
import com.hazelcast.map.impl.query.AccumulationExecutor;
import com.hazelcast.map.impl.query.AggregationResult;
import com.hazelcast.map.impl.query.AggregationResultProcessor;
import com.hazelcast.map.impl.query.CallerRunsAccumulationExecutor;
import com.hazelcast.map.impl.query.CallerRunsPartitionScanExecutor;
import com.hazelcast.map.impl.query.ParallelAccumulationExecutor;
import com.hazelcast.map.impl.query.ParallelPartitionScanExecutor;
import com.hazelcast.map.impl.query.PartitionScanExecutor;
import com.hazelcast.map.impl.query.PartitionScanRunner;
import com.hazelcast.map.impl.query.QueryEngine;
import com.hazelcast.map.impl.query.QueryEngineImpl;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultProcessor;
import com.hazelcast.map.impl.query.QueryRunner;
import com.hazelcast.map.impl.query.ResultProcessorRegistry;
import com.hazelcast.map.impl.querycache.NodeQueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.query.impl.DefaultIndexProvider;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.IndexProvider;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.SetUtil.immutablePartitionIdSet;
import static com.hazelcast.map.impl.ListenerAdapters.createListenerAdapter;
import static com.hazelcast.map.impl.MapKeyLoader.LOADED_KEY_LIMITER_PER_NODE;
import static com.hazelcast.map.impl.MapKeyLoader.PROP_LOADED_KEY_LIMITER_PER_NODE;
import static com.hazelcast.map.impl.MapListenerFlagOperator.setAndGetListenerFlags;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.query.impl.predicates.QueryOptimizerFactory.newOptimizer;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.QUERY_EXECUTOR;
import static com.hazelcast.spi.impl.operationservice.Operation.GENERIC_PARTITION_ID;
import static com.hazelcast.spi.properties.ClusterProperty.AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION;
import static com.hazelcast.spi.properties.ClusterProperty.EXPENSIVE_IMAP_INVOCATION_REPORTING_THRESHOLD;
import static com.hazelcast.spi.properties.ClusterProperty.INDEX_COPY_BEHAVIOR;
import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.ClusterProperty.QUERY_PREDICATE_PARALLEL_EVALUATION;
import static java.lang.Thread.currentThread;

/**
 * Default implementation of {@link MapServiceContext}.
 */
@SuppressWarnings({"WeakerAccess", "ClassDataAbstractionCoupling", "ClassFanOutComplexity", "MethodCount"})
class MapServiceContextImpl implements MapServiceContext {

    private static final long DESTROY_TIMEOUT_SECONDS = 30;

    protected final ILogger logger;

    private final NodeEngine nodeEngine;
    private final QueryEngine queryEngine;
    private final EventService eventService;
    private final QueryRunner mapQueryRunner;
    private final MapEventJournal eventJournal;
    private final QueryOptimizer queryOptimizer;
    private final MapEventPublisher mapEventPublisher;
    private final QueryCacheContext queryCacheContext;
    private final ExpirationManager expirationManager;
    private final PartitionScanRunner partitionScanRunner;
    private final MapNearCacheManager mapNearCacheManager;
    private final MapOperationProviders operationProviders;
    private final PartitionContainer[] partitionContainers;
    private final LocalMapStatsProvider localMapStatsProvider;
    private final ResultProcessorRegistry resultProcessorRegistry;
    private final InternalSerializationService serializationService;
    private final MapClearExpiredRecordsTask clearExpiredRecordsTask;
    private final PartitioningStrategyFactory partitioningStrategyFactory;
    private final NodeWideUsedCapacityCounter nodeWideUsedCapacityCounter;
    private final ConstructorFunction<String, MapContainer> mapConstructor;
    private final IndexProvider indexProvider = new DefaultIndexProvider();
    private final ContextMutexFactory contextMutexFactory = new ContextMutexFactory();
    private final ConcurrentMap<String, MapContainer> mapContainers = new ConcurrentHashMap<>();
    private final ExecutorStats offloadedExecutorStats = new ExecutorStats();
    private final AtomicReference<PartitionIdSet> cachedOwnedPartitions = new AtomicReference<>();

    /**
     * @see MapKeyLoader#DEFAULT_LOADED_KEY_LIMIT_PER_NODE
     */
    private final Semaphore nodeWideLoadedKeyLimiter;
    private final boolean forceOffloadEnabled;
    private final long maxSuccessiveOffloadedOpRunNanos;
    private final int expensiveInvocationReportingThreshold;

    private MapService mapService;

    @SuppressWarnings("checkstyle:executablestatementcount")
    MapServiceContextImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.serializationService = ((InternalSerializationService) nodeEngine.getSerializationService());
        this.mapConstructor = createMapConstructor();
        this.queryCacheContext = new NodeQueryCacheContext(this);
        this.partitionContainers = createPartitionContainers();
        this.clearExpiredRecordsTask = new MapClearExpiredRecordsTask(partitionContainers, nodeEngine);
        this.expirationManager = new ExpirationManager(clearExpiredRecordsTask, nodeEngine);
        this.mapNearCacheManager = createMapNearCacheManager();
        this.localMapStatsProvider = createLocalMapStatsProvider();
        this.mapEventPublisher = createMapEventPublisherSupport();
        this.eventJournal = createEventJournal();
        this.queryOptimizer = newOptimizer(nodeEngine.getProperties());
        this.resultProcessorRegistry = createResultProcessorRegistry(serializationService);
        this.partitionScanRunner = createPartitionScanRunner();
        this.queryEngine = createMapQueryEngine();
        this.mapQueryRunner = createMapQueryRunner(nodeEngine, queryOptimizer,
                resultProcessorRegistry, partitionScanRunner);
        this.eventService = nodeEngine.getEventService();
        this.operationProviders = createOperationProviders();
        this.partitioningStrategyFactory = new PartitioningStrategyFactory(nodeEngine.getConfigClassLoader());
        this.nodeWideUsedCapacityCounter = new NodeWideUsedCapacityCounter(nodeEngine.getProperties());
        this.nodeWideLoadedKeyLimiter = new Semaphore(checkPositive(PROP_LOADED_KEY_LIMITER_PER_NODE,
                nodeEngine.getProperties().getInteger(LOADED_KEY_LIMITER_PER_NODE)));
        this.logger = nodeEngine.getLogger(getClass());
        this.forceOffloadEnabled = nodeEngine.getProperties()
                .getBoolean(FORCE_OFFLOAD_ALL_OPERATIONS);
        this.maxSuccessiveOffloadedOpRunNanos = nodeEngine.getProperties()
                .getNanos(MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS);
        this.expensiveInvocationReportingThreshold = nodeEngine.getProperties()
                .getInteger(EXPENSIVE_IMAP_INVOCATION_REPORTING_THRESHOLD);
        if (this.forceOffloadEnabled) {
            logger.info("Force offload is enabled for all maps. This "
                    + "means all map operations will run as if they have map-store configured. "
                    + "The intended usage for this flag is testing purposes.");
        }
    }

    @Override
    public boolean isForceOffloadEnabled() {
        return forceOffloadEnabled;
    }

    @Override
    public long getMaxSuccessiveOffloadedOpRunNanos() {
        return maxSuccessiveOffloadedOpRunNanos;
    }

    @Override
    public ExecutorStats getOffloadedEntryProcessorExecutorStats() {
        return offloadedExecutorStats;
    }

    private ConstructorFunction<String, MapContainer> createMapConstructor() {
        return mapName -> {
            MapContainer mapContainer = createMapContainer(mapName);
            mapContainer.init();
            return mapContainer;
        };
    }

    // this method is overridden in another context
    MapContainer createMapContainer(String mapName) {
        MapServiceContext mapServiceContext = getService().getMapServiceContext();
        return new MapContainerImpl(mapName, nodeEngine.getConfig(), mapServiceContext);
    }

    // this method is overridden in another context
    MapNearCacheManager createMapNearCacheManager() {
        return new MapNearCacheManager(this);
    }

    // this method is overridden in another context
    MapOperationProviders createOperationProviders() {
        return new MapOperationProviders();
    }

    // this method is overridden in another context
    MapEventPublisherImpl createMapEventPublisherSupport() {
        return new MapEventPublisherImpl(this);
    }

    private MapEventJournal createEventJournal() {
        return new RingbufferMapEventJournalImpl(getNodeEngine(), this);
    }

    protected LocalMapStatsProvider createLocalMapStatsProvider() {
        return new LocalMapStatsProvider(this);
    }

    private QueryEngineImpl createMapQueryEngine() {
        return new QueryEngineImpl(this);
    }

    private PartitionScanRunner createPartitionScanRunner() {
        return new PartitionScanRunner(this);
    }

    protected QueryRunner createMapQueryRunner(NodeEngine nodeEngine, QueryOptimizer queryOptimizer,
                                               ResultProcessorRegistry resultProcessorRegistry,
                                               PartitionScanRunner partitionScanRunner) {
        boolean parallelEvaluation = nodeEngine.getProperties().getBoolean(QUERY_PREDICATE_PARALLEL_EVALUATION);
        PartitionScanExecutor partitionScanExecutor;
        if (parallelEvaluation) {
            int opTimeoutInMillis = nodeEngine.getProperties().getInteger(OPERATION_CALL_TIMEOUT_MILLIS);
            ManagedExecutorService queryExecutorService = nodeEngine.getExecutionService().getExecutor(QUERY_EXECUTOR);
            partitionScanExecutor = new ParallelPartitionScanExecutor(partitionScanRunner, queryExecutorService,
                    opTimeoutInMillis);
        } else {
            partitionScanExecutor = new CallerRunsPartitionScanExecutor(partitionScanRunner);
        }
        return new QueryRunner(this, queryOptimizer, partitionScanExecutor, resultProcessorRegistry);
    }

    private ResultProcessorRegistry createResultProcessorRegistry(SerializationService ss) {
        ResultProcessorRegistry registry = new ResultProcessorRegistry();
        registry.registerProcessor(QueryResult.class, createQueryResultProcessor(ss));
        registry.registerProcessor(AggregationResult.class, createAggregationResultProcessor(ss));
        return registry;
    }

    private QueryResultProcessor createQueryResultProcessor(SerializationService ss) {
        return new QueryResultProcessor(ss);
    }

    private AggregationResultProcessor createAggregationResultProcessor(SerializationService ss) {
        boolean parallelAccumulation = nodeEngine.getProperties().getBoolean(AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION);
        int opTimeoutInMillis = nodeEngine.getProperties().getInteger(OPERATION_CALL_TIMEOUT_MILLIS);
        AccumulationExecutor accumulationExecutor;
        if (parallelAccumulation) {
            ManagedExecutorService queryExecutorService = nodeEngine.getExecutionService().getExecutor(QUERY_EXECUTOR);
            accumulationExecutor = new ParallelAccumulationExecutor(queryExecutorService, ss, opTimeoutInMillis);
        } else {
            accumulationExecutor = new CallerRunsAccumulationExecutor(ss);
        }

        return new AggregationResultProcessor(accumulationExecutor, serializationService);
    }

    private PartitionContainer[] createPartitionContainers() {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        return new PartitionContainer[partitionCount];
    }

    @Override
    public MapContainer getMapContainer(String mapName) {
        return ConcurrencyUtil.getOrPutSynchronized(mapContainers, mapName, contextMutexFactory, mapConstructor);
    }

    @Override
    public MapContainer getExistingMapContainer(String mapName) {
        return mapContainers.get(mapName);
    }

    @Override
    public Map<String, MapContainer> getMapContainers() {
        return mapContainers;
    }

    @Override
    public PartitionContainer getPartitionContainer(int partitionId) {
        assert partitionId != GENERIC_PARTITION_ID : "Cannot be called with GENERIC_PARTITION_ID";

        return partitionContainers[partitionId];
    }

    @Override
    public void initPartitionsContainers() {
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = createPartitionContainer(getService(), i);
        }
    }

    protected PartitionContainer createPartitionContainer(MapService service, int partitionId) {
        return new PartitionContainerImpl(service, partitionId);
    }

    /**
     * Removes all record stores from all partitions.
     *
     * Calls {@link #removeRecordStoresFromPartitionMatchingWith} internally and
     *
     * @param onShutdown           {@code true} if this method is called during map service shutdown,
     *                             otherwise set {@code false}
     * @param onRecordStoreDestroy {@code true} if this method is called during to destroy record store,
     *                             otherwise set {@code false}
     */
    protected void removeAllRecordStoresOfAllMaps(boolean onShutdown, boolean onRecordStoreDestroy) {
        for (PartitionContainer partitionContainer : partitionContainers) {
            if (partitionContainer != null) {
                removeRecordStoresFromPartitionMatchingWith(recordStore -> true,
                        partitionContainer.getPartitionId(), onShutdown, onRecordStoreDestroy);
            }
        }
    }

    @Override
    public final void removeRecordStoresFromPartitionMatchingWith(Predicate<RecordStore> predicate,
                                                                  int partitionId,
                                                                  boolean onShutdown,
                                                                  boolean onRecordStoreDestroy) {

        PartitionContainer container = partitionContainers[partitionId];
        if (container == null) {
            return;
        }

        Queue<Disposable> disposalQueue = new ArrayDeque<>();
        List<RecordStore> removedRecordStores = new ArrayList<>();
        Iterator<RecordStore> recordStoreIterator = container.getMaps().values().iterator();
        while (recordStoreIterator.hasNext()) {
            RecordStore recordStore = recordStoreIterator.next();
            if (predicate.test(recordStore)) {
                // First, remove the record store to reduce the chance of access
                // from non-partition threads. See `MapContainerImpl#hasNotExpired`,
                // which may be called by query threads.
                recordStoreIterator.remove();

                recordStore.beforeOperation();
                try {
                    // isEmpty can throw an exception here if there are any active loads on the recordStore
                    @SuppressWarnings({"SizeReplaceableByIsEmpty"})
                    boolean empty = recordStore.size() == 0;

                    recordStore.clearPartition(onShutdown, onRecordStoreDestroy, disposalQueue);

                    if (!empty) {
                        removedRecordStores.add(recordStore);
                    }
                } finally {
                    recordStore.afterOperation();
                }
            }
        }

        postProcessNonEmptyRemovedRecordStores(removedRecordStores, partitionId, onShutdown);
        processDisposalQueue(disposalQueue, partitionId, onShutdown);
    }

    protected void processDisposalQueue(Queue<Disposable> disposalQueue,
                                        int partitionId, boolean onShutdown) {
        // NOP
    }

    protected void postProcessNonEmptyRemovedRecordStores(List<RecordStore> removedRecordStores,
                                                          int partitionId, boolean onShutdown) {
        // NOP
    }

    @Override
    public void removeWbqCountersFromMatchingPartitionsWith(Predicate<RecordStore> predicate,
                                                            int partitionId) {
        PartitionContainer container = partitionContainers[partitionId];
        if (container == null) {
            return;
        }

        Iterator<RecordStore> partitionIterator = container.getMaps().values().iterator();
        while (partitionIterator.hasNext()) {
            RecordStore partition = partitionIterator.next();
            if (predicate.test(partition)) {
                partition.getMapDataStore().getTxnReservedCapacityCounter().releaseAllReservations();
            }
        }
    }

    @Override
    public MapService getService() {
        return mapService;
    }

    @Override
    public void setService(MapService mapService) {
        this.mapService = mapService;
    }

    @Override
    public void destroyMapStores() {
        for (MapContainer mapContainer : mapContainers.values()) {
            MapStoreWrapper store = mapContainer.getMapStoreContext().getMapStoreWrapper();
            if (store != null) {
                store.destroy();
            }
        }
    }

    @Override
    public void flushMaps() {
        for (MapContainer mapContainer : mapContainers.values()) {
            mapContainer.getMapStoreContext().stop();
        }

        for (PartitionContainer partitionContainer : partitionContainers) {
            for (String mapName : mapContainers.keySet()) {
                RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);
                if (recordStore != null) {
                    MapDataStore mapDataStore = recordStore.getMapDataStore();
                    mapDataStore.hardFlush();
                }
            }
        }
    }

    @Override
    public void destroyMap(String mapName) {
        // on LiteMembers we don't have a MapContainer, but we may have a Near Cache and listeners
        mapNearCacheManager.destroyNearCache(mapName);
        nodeEngine.getEventService().deregisterAllLocalListeners(SERVICE_NAME, mapName);

        MapContainer mapContainer = mapContainers.get(mapName);
        if (mapContainer == null) {
            // Lite members create their own LocalMapStatsImpl whenever a new IMap is created,
            // which can happen without a MapContainer, so we need to clean them up - since cleanup
            // is just a simple map entry removal, we can call it without any Lite member checks
            localMapStatsProvider.destroyLocalMapStatsImpl(mapName);
            return;
        }

        nodeEngine.getWanReplicationService().removeWanEventCounters(MapService.SERVICE_NAME, mapName);
        MapStoreContext mapStoreContext = mapContainer.getMapStoreContext();
        mapStoreContext.stop();
        MapStoreWrapper mapStoreWrapper = mapStoreContext.getMapStoreWrapper();
        //If the map has a MapStore, destroy that as well
        if (mapStoreWrapper != null) {
            mapStoreWrapper.destroy();
        }

        // if there are any dynamic indexes, remove them
        removeMapIndexConfigs(mapName);

        // Statistics are destroyed after container to prevent their leak.
        destroyPartitionsAndMapContainer(mapContainer);
    }

    protected void removeMapIndexConfigs(String mapName) {
        // no-op in OS
    }

    /**
     * Destroys the map data on local partition threads and waits for
     * {@value #DESTROY_TIMEOUT_SECONDS} seconds
     * for each partition segment destruction to complete.
     *
     * @param mapContainer the map container to destroy
     */
    private void destroyPartitionsAndMapContainer(MapContainer mapContainer) {
        final List<LocalRetryableExecution> executions = new ArrayList<>();

        for (PartitionContainer partitionContainer : partitionContainers) {
            Operation op = new MapPartitionDestroyOperation(partitionContainer, mapContainer)
                    .setPartitionId(partitionContainer.getPartitionId())
                    .setNodeEngine(nodeEngine)
                    .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                    .setServiceName(SERVICE_NAME);

            executions.add(InvocationUtil.executeLocallyWithRetry(nodeEngine, op));
        }

        for (LocalRetryableExecution execution : executions) {
            try {
                if (!execution.awaitCompletion(DESTROY_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    logger.warning("Map partition was not destroyed in expected time, possible leak");
                }
            } catch (InterruptedException e) {
                currentThread().interrupt();
                nodeEngine.getLogger(getClass()).warning(e);
            }
        }

        mapContainer.onDestroy();
    }

    @Override
    public void reset() {
        removeAllRecordStoresOfAllMaps(false, false);
        mapNearCacheManager.reset();
        offloadedExecutorStats.clear();
    }

    @Override
    public void shutdown() {
        removeAllRecordStoresOfAllMaps(true, false);
        mapNearCacheManager.shutdown();
        mapContainers.clear();
        expirationManager.onShutdown();
        offloadedExecutorStats.clear();
    }

    @Override
    public RecordStore getRecordStore(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getRecordStore(mapName);
    }

    @Override
    public RecordStore getRecordStore(int partitionId, String mapName, boolean skipLoadingOnCreate) {
        return getPartitionContainer(partitionId).getRecordStore(mapName, skipLoadingOnCreate);
    }

    @Override
    public RecordStore getExistingRecordStore(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getExistingRecordStore(mapName);
    }

    @Override
    public PartitionIdSet getCachedOwnedPartitions() {
        PartitionIdSet ownedSet = cachedOwnedPartitions.get();
        if (ownedSet == null) {
            refreshCachedOwnedPartitions();
            ownedSet = cachedOwnedPartitions.get();
        }
        return ownedSet;
    }

    private PartitionIdSet getOwnedMemberPartitions() {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        Collection<Integer> partitions = partitionService.getMemberPartitions(nodeEngine.getThisAddress());
        return immutablePartitionIdSet(partitionService.getPartitionCount(), partitions);
    }

    @SuppressWarnings("checkstyle:multiplevariabledeclarations")
    @Override
    // can be called concurrently
    public void refreshCachedOwnedPartitions() {
        PartitionIdSet expectedSet, newSet;
        do {
            expectedSet = cachedOwnedPartitions.get();
            newSet = getOwnedMemberPartitions();
        } while (!cachedOwnedPartitions.compareAndSet(expectedSet, newSet));
    }

    @Override
    public ExpirationManager getExpirationManager() {
        return expirationManager;
    }

    @Override
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public MapEventPublisher getMapEventPublisher() {
        return mapEventPublisher;
    }

    @Override
    public MapEventJournal getEventJournal() {
        return eventJournal;
    }

    @Override
    public QueryEngine getQueryEngine(String mapName) {
        return queryEngine;
    }

    @Override
    public QueryRunner getMapQueryRunner(String name) {
        return mapQueryRunner;
    }

    @Override
    public QueryOptimizer getQueryOptimizer() {
        return queryOptimizer;
    }

    @Override
    public LocalMapStatsProvider getLocalMapStatsProvider() {
        return localMapStatsProvider;
    }

    @Override
    public Object toObject(Object data) {
        return serializationService.toObject(data);
    }

    @Override
    public Data toData(Object object, PartitioningStrategy partitionStrategy) {
        return serializationService.toData(object, partitionStrategy);
    }

    @Override
    public Data toData(Object object) {
        return serializationService.toData(object, DataType.HEAP);
    }

    @Override
    public MapClearExpiredRecordsTask getClearExpiredRecordsTask() {
        return clearExpiredRecordsTask;
    }

    // TODO: interceptors should get a wrapped object which includes the serialized version
    @Override
    public Object interceptGet(InterceptorRegistry interceptorRegistry, Object currentValue) {
        List<MapInterceptor> interceptors = interceptorRegistry.getInterceptors();
        if (interceptors.isEmpty()) {
            return currentValue;
        }

        Object result = toObject(currentValue);
        for (MapInterceptor interceptor : interceptors) {
            Object temp = interceptor.interceptGet(result);
            if (temp != null) {
                result = temp;
            }
        }
        return result == null ? currentValue : result;
    }

    @Override
    public void interceptAfterGet(InterceptorRegistry interceptorRegistry, Object value) {
        List<MapInterceptor> interceptors = interceptorRegistry.getInterceptors();
        if (interceptors.isEmpty()) {
            return;
        }

        value = toObject(value);
        for (MapInterceptor interceptor : interceptors) {
            interceptor.afterGet(value);
        }
    }

    @Override
    public Object interceptPut(InterceptorRegistry interceptorRegistry, Object oldValue, Object newValue) {
        List<MapInterceptor> interceptors = interceptorRegistry.getInterceptors();
        if (interceptors.isEmpty()) {
            return newValue;
        }

        Object result = toObject(newValue);
        oldValue = toObject(oldValue);
        for (MapInterceptor interceptor : interceptors) {
            Object temp = interceptor.interceptPut(oldValue, result);
            if (temp != null) {
                result = temp;
            }
        }
        return result;
    }

    @Override
    public void interceptAfterPut(InterceptorRegistry interceptorRegistry, Object newValue) {
        List<MapInterceptor> interceptors = interceptorRegistry.getInterceptors();
        if (interceptors.isEmpty()) {
            return;
        }

        newValue = toObject(newValue);
        for (MapInterceptor interceptor : interceptors) {
            interceptor.afterPut(newValue);
        }
    }

    @Override
    public Object interceptRemove(InterceptorRegistry interceptorRegistry, Object value) {
        List<MapInterceptor> interceptors = interceptorRegistry.getInterceptors();
        if (interceptors.isEmpty()) {
            return value;
        }

        Object result = toObject(value);
        for (MapInterceptor interceptor : interceptors) {
            Object temp = interceptor.interceptRemove(result);
            if (temp != null) {
                result = temp;
            }
        }
        return result;
    }

    @Override
    public void interceptAfterRemove(InterceptorRegistry interceptorRegistry, Object value) {
        List<MapInterceptor> interceptors = interceptorRegistry.getInterceptors();
        if (interceptors.isEmpty()) {
            return;
        }

        value = toObject(value);
        for (MapInterceptor interceptor : interceptors) {
            interceptor.afterRemove(value);
        }
    }

    @Override
    public void addInterceptor(String id, String mapName, MapInterceptor interceptor) {
        MapContainer mapContainer = getMapContainer(mapName);
        mapContainer.getInterceptorRegistry().register(id, interceptor);
    }

    @Override
    public boolean removeInterceptor(String mapName, String id) {
        MapContainer mapContainer = getMapContainer(mapName);
        return mapContainer.getInterceptorRegistry().deregister(id);
    }

    @Override
    public String generateInterceptorId(String mapName, MapInterceptor interceptor) {
        return interceptor.getClass().getName() + interceptor.hashCode();
    }

    @Override
    public UUID addLocalEventListener(Object listener, EventFilter eventFilter, String mapName) {
        EventRegistration registration = addListenerInternal(listener, eventFilter, mapName, true);
        return registration.getId();
    }

    @Override
    public UUID addLocalPartitionLostListener(MapPartitionLostListener listener, String mapName) {
        ListenerAdapter listenerAdapter = new InternalMapPartitionLostListenerAdapter(listener);
        EventFilter filter = new MapPartitionLostEventFilter();
        EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, mapName, filter, listenerAdapter);
        return registration.getId();
    }

    @Override
    public UUID addEventListener(Object listener, EventFilter eventFilter, String mapName) {
        EventRegistration registration = addListenerInternal(listener, eventFilter, mapName, false);
        return registration.getId();
    }

    @Override
    public CompletableFuture<UUID> addEventListenerAsync(Object mapListener, EventFilter eventFilter, String mapName) {
        return addListenerInternalAsync(mapListener, eventFilter, mapName);
    }

    @Override
    public UUID addPartitionLostListener(MapPartitionLostListener listener, String mapName) {
        ListenerAdapter listenerAdapter = new InternalMapPartitionLostListenerAdapter(listener);
        EventFilter filter = new MapPartitionLostEventFilter();
        EventRegistration registration = eventService.registerListener(SERVICE_NAME, mapName, filter, listenerAdapter);
        return registration.getId();
    }

    @Override
    public CompletableFuture<UUID> addPartitionLostListenerAsync(MapPartitionLostListener listener, String mapName) {
        ListenerAdapter listenerAdapter = new InternalMapPartitionLostListenerAdapter(listener);
        EventFilter filter = new MapPartitionLostEventFilter();
        return eventService.registerListenerAsync(SERVICE_NAME, mapName, filter, listenerAdapter)
                .thenApplyAsync(EventRegistration::getId, CALLER_RUNS);
    }

    private EventRegistration addListenerInternal(Object listener, EventFilter filter, String mapName, boolean local) {
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        filter = adoptEventFilter(filter, listenerAdaptor);

        if (local) {
            return eventService.registerLocalListener(SERVICE_NAME, mapName, filter, listenerAdaptor);
        } else {
            return eventService.registerListener(SERVICE_NAME, mapName, filter, listenerAdaptor);
        }
    }

    private CompletableFuture<UUID> addListenerInternalAsync(Object listener, EventFilter filter, String mapName) {
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        filter = adoptEventFilter(filter, listenerAdaptor);
        return eventService.registerListenerAsync(SERVICE_NAME, mapName, filter, listenerAdaptor)
                .thenApplyAsync(EventRegistration::getId, CALLER_RUNS);
    }

    private EventFilter adoptEventFilter(EventFilter filter, ListenerAdapter listenerAdaptor) {
        if (!(filter instanceof EventListenerFilter)) {
            int enabledListeners = setAndGetListenerFlags(listenerAdaptor);
            filter = new EventListenerFilter(enabledListeners, filter);
        }
        return filter;
    }

    @Override
    public boolean removeEventListener(String mapName, UUID registrationId) {
        return eventService.deregisterListener(SERVICE_NAME, mapName, registrationId);
    }

    @Override
    public CompletableFuture<Boolean> removeEventListenerAsync(String mapName, UUID registrationId) {
        return eventService.deregisterListenerAsync(SERVICE_NAME, mapName, registrationId);
    }

    @Override
    public boolean removePartitionLostListener(String mapName, UUID registrationId) {
        return eventService.deregisterListener(SERVICE_NAME, mapName, registrationId);
    }

    @Override
    public CompletableFuture<Boolean> removePartitionLostListenerAsync(String mapName, UUID registrationId) {
        return eventService.deregisterListenerAsync(SERVICE_NAME, mapName, registrationId);
    }

    @Override
    public MapOperationProvider getMapOperationProvider(String mapName) {
        return operationProviders.getOperationProvider(mapName);
    }

    @Override
    public IndexProvider getIndexProvider(MapConfig mapConfig) {
        return indexProvider;
    }

    @Override
    public Extractors getExtractors(String mapName) {
        MapContainer mapContainer = getMapContainer(mapName);
        return mapContainer.getExtractors();
    }

    @Override
    public RecordStore createRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader) {
        assert partitionId != GENERIC_PARTITION_ID : "Cannot be called with GENERIC_PARTITION_ID";

        ILogger logger = nodeEngine.getLogger(DefaultRecordStore.class);
        return new DefaultRecordStore(mapContainer, partitionId, keyLoader, logger);
    }

    @Override
    public boolean removeMapContainer(MapContainer mapContainer) {
        return mapContainers.remove(mapContainer.getName(), mapContainer);
    }

    @Override
    @Nullable
    public PartitioningStrategy getPartitioningStrategy(
            String mapName,
            PartitioningStrategyConfig config,
            final List<PartitioningAttributeConfig> partitioningAttributeConfigs
    ) {
        return partitioningStrategyFactory.getPartitioningStrategy(mapName, config, partitioningAttributeConfigs);
    }

    @Override
    public void removePartitioningStrategyFromCache(String mapName) {
        partitioningStrategyFactory.removePartitioningStrategyFromCache(mapName);
    }

    @Override
    public PartitionContainer[] getPartitionContainers() {
        return partitionContainers;
    }

    @Override
    public void onClusterStateChange(ClusterState newState) {
        expirationManager.onClusterStateChange(newState);
    }

    @Override
    public ResultProcessorRegistry getResultProcessorRegistry() {
        return resultProcessorRegistry;
    }

    @Override
    public MapNearCacheManager getMapNearCacheManager() {
        return mapNearCacheManager;
    }

    @Override
    public UUID addListenerAdapter(ListenerAdapter listenerAdaptor, EventFilter eventFilter, String mapName) {
        EventRegistration registration = getNodeEngine().getEventService().
                registerListener(MapService.SERVICE_NAME, mapName, eventFilter, listenerAdaptor);
        return registration.getId();
    }

    @Override
    public CompletableFuture<UUID> addListenerAdapterAsync(ListenerAdapter listenerAdaptor, EventFilter eventFilter,
                                                           String mapName) {
        return getNodeEngine().getEventService()
                .registerListenerAsync(MapService.SERVICE_NAME, mapName, eventFilter, listenerAdaptor)
                .thenApplyAsync(EventRegistration::getId, CALLER_RUNS);
    }

    @Override
    public UUID addLocalListenerAdapter(ListenerAdapter adapter, String mapName) {
        EventService eventService = getNodeEngine().getEventService();
        EventRegistration registration = eventService.registerLocalListener(MapService.SERVICE_NAME, mapName, adapter);
        return registration.getId();
    }

    @Override
    public QueryCacheContext getQueryCacheContext() {
        return queryCacheContext;
    }

    @Override
    public IndexCopyBehavior getIndexCopyBehavior() {
        return nodeEngine.getProperties().getEnum(INDEX_COPY_BEHAVIOR, IndexCopyBehavior.class);
    }

    @Override
    public boolean globalIndexEnabled() {
        return true;
    }

    @Override
    public boolean isForciblyEnabledGlobalIndex() {
        return false;
    }

    @Override
    public ValueComparator getValueComparatorOf(InMemoryFormat inMemoryFormat) {
        return ValueComparatorUtil.getValueComparatorOf(inMemoryFormat);
    }

    @Override
    public Semaphore getNodeWideLoadedKeyLimiter() {
        return nodeWideLoadedKeyLimiter;
    }

    @Override
    public NodeWideUsedCapacityCounter getNodeWideUsedCapacityCounter() {
        return nodeWideUsedCapacityCounter;
    }

    // used only for testing purposes
    PartitioningStrategyFactory getPartitioningStrategyFactory() {
        return partitioningStrategyFactory;
    }

    @Override
    public int getExpensiveInvocationReportingThreshold() {
        return expensiveInvocationReportingThreshold;
    }
}
