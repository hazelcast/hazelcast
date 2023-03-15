/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.internal.eviction.ExpirationManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask;
import com.hazelcast.map.impl.journal.MapEventJournal;
import com.hazelcast.map.impl.mapstore.writebehind.NodeWideUsedCapacityCounter;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.query.QueryEngine;
import com.hazelcast.map.impl.query.QueryRunner;
import com.hazelcast.map.impl.query.ResultProcessorRegistry;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.IndexProvider;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.Predicate;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Context which is needed by a map service.
 * <p>
 * Shared instances, configurations of all
 * maps can be reached over this context.
 * <p>
 * Also this context provides some support methods which are used
 * in map operations and {@link RecordStore} implementations. For
 * example all {@link PartitionContainer} and {@link MapContainer}
 * instances can also be reached by using this interface.
 * <p>
 * It is also responsible for providing methods which are used by
 * lower layers of Hazelcast and exposed on {@link MapService}.
 *
 * @see MapManagedService
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public interface MapServiceContext extends MapServiceContextInterceptorSupport,
        MapServiceContextEventListenerSupport {

    /**
     * Following fields for FORCE_OFFLOAD_ALL_OPERATIONS
     * are introduced only for testing purposes.
     *
     * @see {@link MapServiceContext#isForceOffloadEnabled}
     */
    boolean DEFAULT_FORCE_OFFLOAD_ALL_OPERATIONS = false;
    String PROP_FORCE_OFFLOAD_ALL_OPERATIONS
            = "hazelcast.internal.map.force.offload.all.map.operations";
    HazelcastProperty FORCE_OFFLOAD_ALL_OPERATIONS
            = new HazelcastProperty(PROP_FORCE_OFFLOAD_ALL_OPERATIONS,
            DEFAULT_FORCE_OFFLOAD_ALL_OPERATIONS);

    long DEFAULT_MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS = 0;
    String PROP_MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS
            = "hazelcast.internal.map.mapstore.max.successive.offloaded.operation.run.nanos";
    HazelcastProperty MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS
            = new HazelcastProperty(PROP_MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS,
            DEFAULT_MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS, NANOSECONDS);


    Object toObject(Object data);

    Data toData(Object object, PartitioningStrategy partitionStrategy);

    Data toData(Object object);

    MapContainer getMapContainer(String mapName);

    MapContainer getExistingMapContainer(String mapName);

    Map<String, MapContainer> getMapContainers();

    PartitionContainer getPartitionContainer(int partitionId);

    void initPartitionsContainers();

    /**
     * Removes all record stores inside the supplied partition ID matching with
     * the supplied predicate.
     *
     * @param predicate            only matching record-stores with this predicate will be removed
     * @param partitionId          partition ID
     * @param onShutdown           {@code true} if this method is called during map service shutdown,
     *                             otherwise set {@code false}
     * @param onRecordStoreDestroy {@code true} if this method is called during to destroy record store,
     *                             otherwise set {@code false}
     * @see MapManagedService#reset()
     * @see MapManagedService#shutdown(boolean)
     */
    void removeRecordStoresFromPartitionMatchingWith(Predicate<RecordStore> predicate, int partitionId,
                                                     boolean onShutdown, boolean onRecordStoreDestroy);

    /**
     * Removes write-behind-queue-reservation-counters inside
     * supplied partition from matching record-stores.
     *
     * @param predicate   only matching record-stores
     *                    with this predicate will be removed
     * @param partitionId partition ID
     */
    void removeWbqCountersFromMatchingPartitionsWith(Predicate<RecordStore> predicate, int partitionId);

    MapService getService();

    void destroyMapStores();

    void flushMaps();

    void destroyMap(String mapName);

    void reset();

    /**
     * Releases internal resources solely managed by Hazelcast.
     * This method is called when MapService is shutting down.
     */
    void shutdown();

    RecordStore createRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader);

    RecordStore getRecordStore(int partitionId, String mapName);

    RecordStore getRecordStore(int partitionId, String mapName, boolean skipLoadingOnCreate);

    RecordStore getExistingRecordStore(int partitionId, String mapName);

    /**
     * Returns cached collection of owned partitions,
     * When it is null, reloads and caches it again.
     */
    PartitionIdSet getCachedOwnedPartitions();

    void refreshCachedOwnedPartitions();

    ExpirationManager getExpirationManager();

    void setService(MapService mapService);

    NodeEngine getNodeEngine();

    MapEventPublisher getMapEventPublisher();

    MapEventJournal getEventJournal();

    QueryEngine getQueryEngine(String name);

    QueryRunner getMapQueryRunner(String name);

    QueryOptimizer getQueryOptimizer();

    LocalMapStatsProvider getLocalMapStatsProvider();

    MapClearExpiredRecordsTask getClearExpiredRecordsTask();

    MapOperationProvider getMapOperationProvider(String mapName);

    IndexProvider getIndexProvider(MapConfig mapConfig);

    Extractors getExtractors(String mapName);

    boolean removeMapContainer(MapContainer mapContainer);

    PartitioningStrategy getPartitioningStrategy(
            String mapName,
            PartitioningStrategyConfig config,
            List<PartitioningAttributeConfig> partitioningAttributeConfigs
    );

    void removePartitioningStrategyFromCache(String mapName);

    PartitionContainer[] getPartitionContainers();

    void onClusterStateChange(ClusterState newState);

    ResultProcessorRegistry getResultProcessorRegistry();

    MapNearCacheManager getMapNearCacheManager();

    QueryCacheContext getQueryCacheContext();

    UUID addListenerAdapter(ListenerAdapter listenerAdaptor, EventFilter eventFilter, String mapName);

    CompletableFuture<UUID> addListenerAdapterAsync(ListenerAdapter listenerAdaptor, EventFilter eventFilter, String mapName);

    UUID addLocalListenerAdapter(ListenerAdapter listenerAdaptor, String mapName);

    IndexCopyBehavior getIndexCopyBehavior();

    boolean globalIndexEnabled();

    ValueComparator getValueComparatorOf(InMemoryFormat inMemoryFormat);

    NodeWideUsedCapacityCounter getNodeWideUsedCapacityCounter();

    ExecutorStats getOffloadedEntryProcessorExecutorStats();

    /**
     * Only used for testing purposes.
     * <p>
     * Default value is {@code false}
     * <p>
     * Forces offload of all operations of all maps regardless of a
     * map-store is being configured. This has identical behavior
     * with enabling {@link MapStoreConfig#isOffload()} for all maps.
     *
     * @return {@code true} if force offload for all
     * operations are enabled, otherwise {@code false}.
     */
    boolean isForceOffloadEnabled();

    /**
     * By default, returns zero.
     * <p>
     * Independent of the number of queued offloaded operations, a
     * {@link com.hazelcast.map.impl.operation.steps.engine.StepRunner}
     * tries to run all queued operation in one go. This may
     * cause biased usage of partition thread for the favour of
     * operating map. To prevent this, one can put max execution
     * time-limit, so partition
     * operations of other maps don't wait longer but if there
     * is a few maps, this limit can cause increased latencies
     * as a side effect.
     *
     * @see #MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS
     * @see com.hazelcast.map.impl.operation.steps.engine.StepRunner
     */
    long getMaxSuccessiveOffloadedOpRunNanos();

    Semaphore getNodeWideLoadedKeyLimiter();

    /**
     * @return {@code true} when Merkle tree maintenance should be enabled for given {@code mapConfig},
     * otherwise {@code false}.
     */
    default boolean shouldEnableMerkleTree(MapConfig mapConfig, boolean log) {
        return false;
    }

    /**
     * @return {@link EventListenerCounter} object.
     */
    EventListenerCounter getEventListenerCounter();
}
