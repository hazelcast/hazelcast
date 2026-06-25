/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.service;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.partition.ChunkSupplier;
import com.hazelcast.internal.partition.ChunkedMigrationAwareService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.OffloadedReplicationPreparation;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.internal.util.executor.ExecutorType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.impl.VectorCollectionOptimizationManager;
import com.hazelcast.vector.impl.VectorCollectionService;
import com.hazelcast.vector.impl.ops.ReplicationOperation;
import com.hazelcast.vector.impl.proxy.VectorCollectionProxy;
import com.hazelcast.vector.impl.query.PartitionLimitEstimatingSearcher;
import com.hazelcast.vector.impl.query.Searcher;
import com.hazelcast.vector.impl.query.SingleStageSearcher;
import com.hazelcast.vector.impl.query.TwoStageSearcher;
import com.hazelcast.vector.impl.stats.CollectionOnDemandStats;
import com.hazelcast.vector.impl.stats.CollectionOnDemandStatsImpl;
import com.hazelcast.vector.impl.stats.LocalVectorCollectionStatsImpl;
import com.hazelcast.vector.impl.stats.LocalVectorCollectionStatsProvider;
import com.hazelcast.vector.impl.stats.OnDemandStatsImpl;
import com.hazelcast.vector.impl.stats.VectorIndexStatsImpl;
import com.hazelcast.vector.impl.storage.OnHeapVectorCollectionObjectProvider;
import com.hazelcast.vector.impl.storage.VectorCollectionStorage;
import com.hazelcast.vector.impl.storage.VectorIndexFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.internal.cluster.Versions.V5_6;
import static com.hazelcast.internal.config.MergePolicyValidator.checkVectorCollectionMergePolicy;
import static com.hazelcast.internal.util.JVMUtil.OBJECT_HEADER_SIZE;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;
import static com.hazelcast.vector.impl.Hints.FORCE_SINGLE_STAGE_SEARCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings({"ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:MethodCount"})
public class VectorCollectionServiceImpl implements VectorCollectionService,
        ChunkedMigrationAwareService, OffloadedReplicationPreparation,
        SplitBrainProtectionAwareService, SplitBrainHandlerService {

    /**
     * Name of the vector similarity search query executor.
     */
    public static final String VECTOR_QUERY_EXECUTOR = ExecutionService.VECTOR_QUERY_EXECUTOR;

    /**
     * Time after which offloaded operation will free partition thread and schedule further execution later.
     * Supported only by some operations that otherwise might occupy partition thread for a long time (e.g. putAll).
     * If disabled (0) operation runs on partition thread in one go.
     * <p>
     * Note that when offloaded, the operation may not execute atomically and in case of migrations may be only
     * partially executed.
     */
    public static final HazelcastProperty MAX_SUCCESSIVE_OFFLOADED_OP_RUN_MILLIS
            = new HazelcastProperty("hazelcast.vector.max.successive.run.millis", 0, MILLISECONDS);

    /**
     * Number of concurrently allowed optimizations initiated by the user.
     * This is global across all vector collection, partitions and primary/backup replicas.
     * Optimization under the hood uses concurrent execution.
     * In general, it is desirable to make optimization of single partition as fast as possible
     * so the updates on it are blocked for the shortest possible time.
     */
    public static final HazelcastProperty MAX_CONCURRENT_OPTIMIZE
            = new HazelcastProperty("hazelcast.vector.max.concurrent.optimize", 1);

    public static final long FIXED_HEAP_BYTES_USED = OBJECT_HEADER_SIZE + 11L * REFERENCE_COST_IN_BYTES + Long.BYTES
            // add fixed cost of VectorCollectionSplitBrainHandlerService
            + OBJECT_HEADER_SIZE + REFERENCE_COST_IN_BYTES;
    static final long HEAP_BYTES_USED_PER_COLLECTION_NAME = 2L * REFERENCE_COST_IN_BYTES;
    static final long HEAP_BYTES_USED_PER_PARTITION_STORAGE = 2L * REFERENCE_COST_IN_BYTES + Integer.BYTES;
    static final long HEAP_BYTES_USED_PER_DISTRIBUTED_OBJECT_NAME =
            // key in namespaces map
            REFERENCE_COST_IN_BYTES
            // value in namespaces map
            + OBJECT_HEADER_SIZE + 2L * REFERENCE_COST_IN_BYTES;
    static final long HEAP_BYTES_USED_PER_SPLITBRAINPROTECTION_CACHE_ENTRY =
            // two references to String or a String and Object (in case of NULL_OBJECT being the value)
            2L * REFERENCE_COST_IN_BYTES;

    private static final Function<String, ObjectNamespace> OBJECT_NAMESPACE_CONSTRUCTOR =
            VectorCollectionServiceImpl::createObjectNamespace;

    // null object for split-brain protection names cache
    private static final Object NULL_OBJECT = new Object();

    private final Function<String, Object> splitBrainProtectionNameConstructor = name -> {
        var splitBrainProtectionName = getExistingVectorCollectionConfig(name).getSplitBrainProtectionName();
        return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
    };

    private final NodeEngine nodeEngine;
    private final ILogger logger;
    // storage per vector collection name and partitionId
    // Invariant: partially migrated storage shall not be visible on destination. However, it can be visible
    // if it has all the data but migration has not yet been committed.
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, VectorCollectionStorage>> storage;
    // namespaces cache
    private final ConcurrentHashMap<String, ObjectNamespace> namespaces;
    // split brain protection names cache
    private final ConcurrentHashMap<String, Object> splitBrainProtectionNames;

    private final long maxOffloadedRunNanos;
    private final Searcher singleStageSearcher;
    private final Searcher twoStageSearcher;
    private final LocalVectorCollectionStatsProvider statisticsProvider;
    private final VectorCollectionSplitBrainHandlerService splitBrainHandlerService;
    private final VectorCollectionOptimizationManager optimizationManager;

    private final VectorIndexFactory vectorIndexFactory;

    public VectorCollectionServiceImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(VectorCollectionServiceImpl.class);
        this.storage = new ConcurrentHashMap<>();
        this.namespaces = new ConcurrentHashMap<>();
        this.splitBrainProtectionNames = new ConcurrentHashMap<>();
        this.maxOffloadedRunNanos = nodeEngine.getProperties().getNanos(MAX_SUCCESSIVE_OFFLOADED_OP_RUN_MILLIS);
        this.optimizationManager = new VectorCollectionOptimizationManagerImpl(nodeEngine);

        // pre-create some singletons
        singleStageSearcher = new PartitionLimitEstimatingSearcher(nodeEngine, new SingleStageSearcher(nodeEngine));
        twoStageSearcher = new PartitionLimitEstimatingSearcher(nodeEngine, new TwoStageSearcher(nodeEngine));

        // stats
        statisticsProvider = new LocalVectorCollectionStatsProvider(nodeEngine, this);
        // split brain merge handling
        splitBrainHandlerService = new VectorCollectionSplitBrainHandlerService(nodeEngine);

        registerVectorQueryExecutor(nodeEngine);
        vectorIndexFactory = new VectorIndexFactory(nodeEngine);
    }

    @SuppressWarnings("resource")
    private void registerVectorQueryExecutor(NodeEngine nodeEngine) {
        // By default vector search will use half of physical cores (see ADR-00046).
        // With SIMD (Panama) enabled on Intel using more than physical cores has gives a limited improvement in performance.
        // However, to obtain best results the pool size should be configured explicitly.
        // Exceeding number of vCPUs should not be beneficial, as vector search is CPU-bound.
        // See also PhysicalCoreExecutor.
        int physicalCoreCount = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        logger.finest("Registering new vector query executor with default threadCount=%d", physicalCoreCount);
        nodeEngine.getExecutionService().register(VECTOR_QUERY_EXECUTOR,
                physicalCoreCount, Integer.MAX_VALUE, ExecutorType.CACHED);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        boolean dsMetricsEnabled = nodeEngine.getProperties().getBoolean(ClusterProperty.METRICS_DATASTRUCTURES);
        if (dsMetricsEnabled) {
            nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(statisticsProvider);
        }
    }

    @Override
    public void reset() {
        // This implementation is simplified and works with the assumptions described in destroyDistributedObject
        storage.clear();
        namespaces.clear();
        statisticsProvider.reset();
    }

    @Override
    public void shutdown(boolean terminate) {
        // This implementation is simplified and works with the assumptions described in destroyDistributedObject
        storage.clear();
        namespaces.clear();
        vectorIndexFactory.shutdown();
    }

    @Override
    public DistributedObject createDistributedObject(String objectName, UUID source, boolean local) {
        VectorCollectionConfig config = getAndValidateExistingVectorCollectionConfig(objectName);
        // pre-create statistics for a potentially new collection.
        // statistics are created eagerly so empty collection
        // (collection without any data in partitions owned by this member) is also visible in metrics
        getStatistics(objectName);
        return new VectorCollectionProxy<>(nodeEngine, this, objectName, config);
    }

    @Override
    public void destroyDistributedObject(String objectName, boolean local) {
        // This implementation is simplified and works with the following assumptions as it does not explicitly destroy
        // partitions:
        // - supports only on-heap storage (StorageImpl), relies on GC to free the memory. Nice consequence of it is that
        //   concurrently executing searches and optimize will not fail with strange exceptions due to VectorCollectionStorage
        //   being destroyed
        // - concurrent mutating operations can resurrect the storage, but that is allowed in `destroy` anyway
        // - VectorCollectionStorage does not register itself anywhere (metrics, listeners)
        // Should any of these assumptions change (e.g. with HD implementation), proper destruction through partition thread
        // will be needed.
        storage.remove(objectName);
        namespaces.remove(objectName);

        // statistics can exist also if storage does not exist, e.g. on lite members or for empty collections
        // so destroy them independent on storage existence
        statisticsProvider.destroyStatistics(objectName);
    }

    @Override
    public VectorCollectionStorage getStorage(String vectorCollectionName, int partitionId) {
        return storage.computeIfAbsent(vectorCollectionName, name -> new ConcurrentHashMap<>())
                .computeIfAbsent(partitionId,
                        i -> createStorage(vectorCollectionName, partitionId));
    }

    @Override
    public VectorCollectionStorage createStorage(String vectorCollectionName, int partitionId) {
        VectorCollectionConfig config = getAndValidateExistingVectorCollectionConfig(vectorCollectionName);
        // todo: for hd VectorCollectionObjectProvider should depend on config
        return new VectorCollectionStorage(
                nodeEngine,
                vectorCollectionName,
                partitionId,
                config,
                OnHeapVectorCollectionObjectProvider.getInstance(),
                vectorIndexFactory
        );
    }

    @Override
    public void attachStorage(@Nonnull VectorCollectionStorage collectionStorage) {
        var previousStorage = storage.computeIfAbsent(collectionStorage.getName(), name -> new ConcurrentHashMap<>())
                .put(collectionStorage.getPartitionId(), collectionStorage);

        if (previousStorage != null) {
            // This is unexpected, but throwing exception would not stop migration
            // so only log message here to investigate. Data loss might be possible,
            // but only if this member is the owner, which should never happen.
            logger.warning(String.format("Existing storage for collection %s partition %d was replaced",
                    collectionStorage.getName(), collectionStorage.getPartitionId()));

            // TODO: properly destroy VectorCollectionStorage (important for HD)
        }
    }

    @Override
    public void destroyStorage(String vectorCollectionName, int partitionId) {
        var maybeCollection = storage.get(vectorCollectionName);
        if (maybeCollection == null) {
            return;
        }

        // TODO: properly destroy VectorCollectionStorage (important for HD)
        var previous = maybeCollection.remove(partitionId);
        if (previous != null) {
            logger.fine("Destroyed storage for collection %s partition %d", vectorCollectionName, partitionId);
        }
    }

    @Override
    public VectorCollectionStorage getStorageOrNull(String vectorCollectionName, int partitionId) {
        return storage.computeIfAbsent(vectorCollectionName, name -> new ConcurrentHashMap<>())
                .get(partitionId);
    }

    @Nonnull
    private VectorCollectionConfig getAndValidateExistingVectorCollectionConfig(String objectName) {
        VectorCollectionConfig config = getExistingVectorCollectionConfig(objectName);
        validateConfig(config);
        return config;
    }

    @Nonnull
    public VectorCollectionConfig getExistingVectorCollectionConfig(String objectName) {
        VectorCollectionConfig config = nodeEngine.getConfig().getVectorCollectionConfigOrNull(objectName);
        if (config == null) {
            throw new IllegalArgumentException("Configuration for vector collection " + objectName + " does not exist");
        }
        return config;
    }

    private void validateConfig(VectorCollectionConfig config) {
        if (config.getVectorIndexConfigs().isEmpty()) {
            throw new InvalidConfigurationException("Vector collection must have at least one index.");
        }
        // this check applies both to static and dynamic config, so it is done late
        // as the cluster version can change at runtime.
        if (!nodeEngine.getClusterService().getClusterVersion().isGreaterOrEqual(V5_6) && config.getTotalBackupCount() > 0) {
            throw new UnsupportedOperationException(
                    String.format("Vector collection backups require cluster version %s or greater", V5_6));
        }

        checkVectorCollectionMergePolicy(
                config,
                config.getMergePolicyConfig().getPolicy(),
                nodeEngine.getSplitBrainMergePolicyProvider()
        );
    }

    @Nonnull
    @Override
    public Searcher getSearcher(String collectionName, SearchOptions options) {
        // note that the decision here does not affect the correctness of the result:
        // all searchers support all cases. So even after topology change nothing
        // bad will happen, only slightly less efficient execution may be chosen.
        int clusterSize = nodeEngine.getClusterService().getSize(DATA_MEMBER_SELECTOR);
        if (clusterSize <= 1) {
            // with single data member all partitions reside locally,
            // no point in executing 2-stage aggregation.
            return singleStageSearcher;
        }

        if (Boolean.TRUE == FORCE_SINGLE_STAGE_SEARCH.get(options)) {
            return singleStageSearcher;
        }

        return twoStageSearcher;
    }

    @Override
    public LocalVectorCollectionStatsImpl getStatistics(String vectorCollectionName) {
        return statisticsProvider.getStatistics(vectorCollectionName);
    }

    public long getMaxOffloadedRunNanos() {
        return maxOffloadedRunNanos;
    }

    //region metrics

    @Override
    public CollectionOnDemandStats getOnDemandStats(String vectorCollectionName) {
        ConcurrentMap<Integer, VectorCollectionStorage> chm = storage.get(vectorCollectionName);
        return chm == null
                ? getCollectionOnDemandStatsWithoutStorage(vectorCollectionName)
                : getCollectionOnDemandStatsWithStorage(chm);
    }

    @Nonnull
    private CollectionOnDemandStatsImpl getCollectionOnDemandStatsWithoutStorage(String vectorCollectionName) {
        // if there is no storage, there is also no distributed object name cached in namespaces map
        return new CollectionOnDemandStatsImpl(statisticsProvider.hasStatistics(vectorCollectionName)
                ? LocalVectorCollectionStatsProvider.ENTRY_HEAP_BYTES_USED
                : 0);
    }

    @Nonnull
    private CollectionOnDemandStatsImpl getCollectionOnDemandStatsWithStorage(
            ConcurrentMap<Integer, VectorCollectionStorage> chm) {
        var owned = new OnDemandStatsImpl();
        var backup = new OnDemandStatsImpl();
        var aggregatedIndexStats = new VectorIndexStatsImpl();

        chm.forEach((partitionId, storage) -> {
            boolean local = nodeEngine.getPartitionService().getPartition(partitionId, false).isLocal();
            (local ? owned : backup)
                    .add(storage.getOnDemandStats())
                    .addHeapBytes(HEAP_BYTES_USED_PER_PARTITION_STORAGE);
            aggregatedIndexStats.add(storage.getVectorIndexStats());
        });

        var heapBytesUsed = owned.heapBytesUsed() + backup.heapBytesUsed()
                // add usage on collection level which is not assigned to owned or backup partitions
                + HEAP_BYTES_USED_PER_COLLECTION_NAME
                // namespace cost, if storage exists the namespace also should exist
                + HEAP_BYTES_USED_PER_DISTRIBUTED_OBJECT_NAME
                // statistics cost, if storage exists the statistics also should exist
                + LocalVectorCollectionStatsProvider.ENTRY_HEAP_BYTES_USED;

        return new CollectionOnDemandStatsImpl(owned, backup, heapBytesUsed, aggregatedIndexStats);
    }

    @Override
    public long heapBytesUsed() {
        // Results may temporarily be inconsistent during execution of concurrent operations on
        // VectorCollectionStorage instances (e.g. during partition migrations).
        return FIXED_HEAP_BYTES_USED
                + storage.size() * HEAP_BYTES_USED_PER_COLLECTION_NAME
                + storage.values().stream().mapToLong(chm ->
                            chm.values().stream().mapToLong(vcStorage ->
                                    HEAP_BYTES_USED_PER_PARTITION_STORAGE + vcStorage.heapBytesUsed()).sum()).sum()
                + singleStageSearcher.heapBytesUsed()
                + twoStageSearcher.heapBytesUsed()
                + namespaces.size() * HEAP_BYTES_USED_PER_DISTRIBUTED_OBJECT_NAME
                + splitBrainProtectionNames.size() * HEAP_BYTES_USED_PER_SPLITBRAINPROTECTION_CACHE_ENTRY
                + optimizationManager.heapBytesUsed()
                + statisticsProvider.heapBytesUsed();
    }

    //endregion


    //region ChunkedMigrationAwareService implementation

    // Initial implementation assumptions:
    // - index migrated in serialized form, not rebuilt from data
    // - only fragmented migration, chunked will be later
    // - offloading of time-consuming operations, where index optimization is the most costly one
    // - index configuration validation is not a requested feature, but nice to have and is piggybacking
    //   on sending VectorIndexConfig for other purposes in replication messages
    // - concurrent updates should be prevented thank to regular partition migrating flag, searches are allowed

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        Collection<ServiceNamespace> namespaces =
                storage.entrySet().stream()
                        // migrate also partitions with existing but empty storage, so the collection
                        // will not disappear if it used to contain some data and now is empty.
                        .filter(collectionEntry -> isEligibleForReplication(event.getReplicaIndex(),
                                collectionEntry.getValue().get(event.getPartitionId())))
                        .map(collectionEntry -> getObjectNamespace(collectionEntry.getKey()))
                        .collect(Collectors.toCollection(HashSet::new));
        return namespaces;
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        if (namespace instanceof ObjectNamespace ons) {
            return ons.getServiceName().equals(SERVICE_NAME) && storage.containsKey(ons.getObjectName());
        }
        return false;
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event, Collection<ServiceNamespace> namespaces) {
        Map<String, VectorCollectionStorage> migrationData = new HashMap<>();
        for (ServiceNamespace ns : namespaces) {
            if (ns instanceof ObjectNamespace ons) {
                String collectionName = ons.getObjectName();
                VectorCollectionStorage maybeStorage = getStorageOrNull(collectionName, event.getPartitionId());
                if (isEligibleForReplication(event.getReplicaIndex(), maybeStorage)) {
                    migrationData.put(collectionName, maybeStorage);
                }
            }
        }
        return migrationData.isEmpty() ? null
                : new ReplicationOperation(nodeEngine, migrationData, event.getPartitionId(), event.getReplicaIndex());
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event, getAllServiceNamespaces(event));
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        // beforeMigration runs on partition thread, so in theory we could call lockIndexMutation for all indexes here.
        // However:
        // - this is not needed, as mutating operations are rejected by marking partition as migrating by common migration logic
        // - manually initiated optimization may be already running (offloaded), so second lock would fail or had to wait,
        //   and it is simpler to lockIndexMutation during AbstractVectorIndex.prepareForMigration
        // - we would have to find all migrated storages (yet another time) and the lock on index would be kept longer
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        // We should not do here things that may affect search results (eg. building vector index).
        // Otherwise, we could run into problems like https://github.com/hazelcast/hazelcast/pull/12242
        // due to https://github.com/hazelcast/hazelcast/issues/8385.
        //
        // Index on not-yet-commited partition may be visible for searches. However, it is published ready
        // and during migration it is immutable, so it is safe to use it on destination before migration commit.
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearPartitionReplica(event.getPartitionId(), event.getNewReplicaIndex());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartitionReplica(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearPartitionReplica(int partitionId, int durabilityThreshold) {
        for (var entry : storage.entrySet()) {
            var collectionPartitions = entry.getValue();
            VectorCollectionStorage partition = collectionPartitions.get(partitionId);
            if (partition != null && durabilityThreshold > partition.getConfig().getTotalBackupCount()) {
                // TODO: properly destroy VectorCollectionStorage (important for HD)
                collectionPartitions.remove(partitionId);
            }
        }
    }

    // todo currently we implement fragmented migration but not chunked, chunking to be implemented
    @Override
    public ChunkSupplier newChunkSupplier(PartitionReplicationEvent event, Collection<ServiceNamespace> namespaces) {
        assert !ThreadUtil.isRunningOnPartitionThread();
        for (ServiceNamespace ns : namespaces) {
            if (ns instanceof ObjectNamespace ons) {
                String collectionName = ons.getObjectName();
                VectorCollectionStorage maybeStorage = getStorageOrNull(collectionName, event.getPartitionId());
                if (isEligibleForReplication(event.getReplicaIndex(), maybeStorage)) {
                    // perform cleanup first - it can delete some vectors
                    // cleanup can be long, must run offloaded
                    maybeStorage.prepareForMigration();
                }
            }
        }

        return ChunkedMigrationAwareService.super.newChunkSupplier(event, namespaces);
    }

    private boolean isReplicaIndexEligibleForReplication(int replicaIndex, int configuredBackupCount) {
        return replicaIndex <= configuredBackupCount;
    }

    private boolean isEligibleForReplication(int replicaIndex, @Nullable VectorCollectionStorage maybeStorage) {
        return maybeStorage != null
                && isReplicaIndexEligibleForReplication(replicaIndex, maybeStorage.getConfig().getTotalBackupCount());
    }

    @Override
    public boolean shouldOffload() {
        return true;
    }

    //endregion

    @Override
    public String getSplitBrainProtectionName(String name) {
        var splitBrainProtectionName = splitBrainProtectionNames.computeIfAbsent(name, splitBrainProtectionNameConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    @Override
    public Runnable prepareMergeRunnable() {
        return splitBrainHandlerService.prepareMergeRunnable();
    }

    @Nonnull
    @Override
    public Set<String> getAllExistingVectorCollectionNames() {
        // collections with storage
        HashSet<String> allNames = new HashSet<>(storage.keySet());
        // collections without storage
        allNames.addAll(nodeEngine.getProxyService().getDistributedObjectNames(SERVICE_NAME));

        return allNames;
    }

    @Override
    public ObjectNamespace getObjectNamespace(String collectionName) {
        return namespaces.computeIfAbsent(collectionName, OBJECT_NAMESPACE_CONSTRUCTOR);
    }

    @Override
    public VectorCollectionOptimizationManager getOptimizationManager() {
        return optimizationManager;
    }

    // used only for migration
    public VectorIndexFactory getVectorIndexFactory() {
        return vectorIndexFactory;
    }

    /**
     * @return an {@link Iterator} of {@link VectorCollectionStorage} instances that exist for given {@code partitionId}
     */
    Iterator<VectorCollectionStorage> storageIterator(int partitionId) {
        List<VectorCollectionStorage> storages = new ArrayList<>();
        for (var entry : storage.entrySet()) {
            if (entry.getValue().containsKey(partitionId)) {
                storages.add(entry.getValue().get(partitionId));
            }
        }
        return storages.iterator();
    }

    private static ObjectNamespace createObjectNamespace(String collectionName) {
        return new DistributedObjectNamespace(SERVICE_NAME, collectionName);
    }
}
