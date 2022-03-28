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

package com.hazelcast.map.impl;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanSyncConfig;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.PostJoinAwareService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.MemoryInfoAccessor;
import com.hazelcast.internal.util.RuntimeMemoryInfoAccessor;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.map.impl.eviction.EvictionChecker;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.eviction.EvictorImpl;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.nearcache.invalidation.InvalidationListener;
import com.hazelcast.map.impl.query.QueryEntryFactory;
import com.hazelcast.map.impl.record.DataRecordFactory;
import com.hazelcast.map.impl.record.ObjectRecordFactory;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.impl.record.RecordFactoryAttributes;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.wan.impl.DelegatingWanScheme;
import com.hazelcast.wan.impl.WanReplicationService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.config.MergePolicyValidator.checkMapMergePolicy;
import static com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider.getEvictionPolicyComparator;
import static com.hazelcast.map.impl.eviction.Evictor.NULL_EVICTOR;
import static com.hazelcast.map.impl.mapstore.MapStoreContextFactory.createMapStoreContext;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_EVICTION_BATCH_SIZE;
import static java.lang.Boolean.TRUE;
import static java.lang.System.getProperty;

/**
 * Map container for a map with a specific name. Contains config and
 * supporting structures for all of the maps' functionalities.
 */
@SuppressWarnings({"WeakerAccess", "checkstyle:classfanoutcomplexity"})
public class MapContainer {

    protected final String name;
    protected final String splitBrainProtectionName;
    // on-heap indexes are global, meaning there is only one index per map,
    // stored in the mapContainer, so if globalIndexes is null it means that
    // global index is not in use
    protected final Indexes globalIndexes;
    protected final Extractors extractors;
    protected final MapStoreContext mapStoreContext;
    protected final ObjectNamespace objectNamespace;
    protected final MapServiceContext mapServiceContext;
    protected final QueryEntryFactory queryEntryFactory;
    protected final EventJournalConfig eventJournalConfig;
    protected final PartitioningStrategy partitioningStrategy;
    protected final InternalSerializationService serializationService;
    protected final Function<Object, Data> toDataFunction = new ObjectToData();
    protected final InterceptorRegistry interceptorRegistry = new InterceptorRegistry();
    protected final ConstructorFunction<RecordFactoryAttributes, RecordFactory> recordFactoryConstructor;
    /**
     * Holds number of registered {@link InvalidationListener} from clients.
     */
    protected final AtomicInteger invalidationListenerCounter;
    protected final AtomicLong lastInvalidMergePolicyCheckTime = new AtomicLong();

    protected SplitBrainMergePolicy wanMergePolicy;
    protected DelegatingWanScheme wanReplicationDelegate;

    protected volatile MapConfig mapConfig;
    private volatile Evictor evictor;

    private boolean persistWanReplicatedData;

    private volatile boolean destroyed;

    /**
     * Operations which are done in this constructor should obey the rules defined
     * in the method comment {@link PostJoinAwareService#getPostJoinOperation()}
     * Otherwise undesired situations, like deadlocks, may appear.
     */
    @SuppressWarnings("checkstyle:executablestatementcount")
    public MapContainer(final String name, final Config config, final MapServiceContext mapServiceContext) {
        this.name = name;
        this.mapConfig = config.findMapConfig(name);
        this.eventJournalConfig = mapConfig.getEventJournalConfig();
        this.mapServiceContext = mapServiceContext;
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.partitioningStrategy = createPartitioningStrategy();
        this.splitBrainProtectionName = mapConfig.getSplitBrainProtectionName();
        this.serializationService = ((InternalSerializationService) nodeEngine.getSerializationService());
        this.recordFactoryConstructor = createRecordFactoryConstructor(serializationService);
        this.objectNamespace = MapService.getObjectNamespace(name);
        this.extractors = Extractors.newBuilder(serializationService)
                .setAttributeConfigs(mapConfig.getAttributeConfigs())
                .setClassLoader(nodeEngine.getConfigClassLoader())
                .build();
        this.queryEntryFactory = new QueryEntryFactory(mapConfig.getCacheDeserializedValues(),
                serializationService, extractors);
        this.globalIndexes = shouldUseGlobalIndex() ? createIndexes(true) : null;
        this.mapStoreContext = createMapStoreContext(this);
        this.invalidationListenerCounter = mapServiceContext.getEventListenerCounter()
                .getOrCreateCounter(name);
        initWanReplication(mapServiceContext.getNodeEngine());
    }

    public void init() {
        initEvictor();
        mapStoreContext.start();
    }

    /**
     * @param global set {@code true} to create global indexes, otherwise set
     *               {@code false} to have partitioned indexes
     * @return a new Indexes object
     */
    public Indexes createIndexes(boolean global) {
        int partitionCount = mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount();

        return Indexes.newBuilder(serializationService, mapServiceContext.getIndexCopyBehavior(), mapConfig.getInMemoryFormat())
                .global(global)
                .extractors(extractors)
                .statsEnabled(mapConfig.isStatisticsEnabled())
                .indexProvider(mapServiceContext.getIndexProvider(mapConfig))
                .usesCachedQueryableEntries(mapConfig.getCacheDeserializedValues() != CacheDeserializedValues.NEVER)
                .partitionCount(partitionCount)
                .resultFilterFactory(new IndexResultFilterFactory())
                .build();
    }

    public AtomicLong getLastInvalidMergePolicyCheckTime() {
        return lastInvalidMergePolicyCheckTime;
    }

    private class IndexResultFilterFactory implements Supplier<Predicate<QueryableEntry>> {

        @Override
        public Predicate<QueryableEntry> get() {
            return new Predicate<QueryableEntry>() {
                private long nowInMillis = Clock.currentTimeMillis();

                @Override
                public boolean test(QueryableEntry queryableEntry) {
                    return MapContainer.this.hasNotExpired(queryableEntry, nowInMillis);
                }
            };
        }
    }

    /**
     * @return {@code true} if queryableEntry has
     * not expired, otherwise returns {@code false}
     */
    private boolean hasNotExpired(QueryableEntry queryableEntry, long now) {
        Data keyData = queryableEntry.getKeyData();
        IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        int partitionId = partitionService.getPartitionId(keyData);

        if (!getIndexes(partitionId).isGlobal()) {
            ThreadUtil.assertRunningOnPartitionThread();
        }

        RecordStore recordStore = mapServiceContext.getExistingRecordStore(partitionId, name);
        return recordStore != null
                && !recordStore.isExpired(keyData, now, false);
    }

    public final void initEvictor() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EvictionPolicyComparator evictionPolicyComparator
                = getEvictionPolicyComparator(mapConfig.getEvictionConfig(), nodeEngine.getConfigClassLoader());

        evictor = evictionPolicyComparator != null
                ? newEvictor(evictionPolicyComparator, nodeEngine.getProperties().getInteger(MAP_EVICTION_BATCH_SIZE),
                nodeEngine.getPartitionService()) : NULL_EVICTOR;
    }

    // this method is overridden
    protected Evictor newEvictor(EvictionPolicyComparator evictionPolicyComparator,
                                 int evictionBatchSize, IPartitionService partitionService) {
        EvictionChecker evictionChecker = new EvictionChecker(getMemoryInfoAccessor(), mapServiceContext);

        return new EvictorImpl(evictionPolicyComparator, evictionChecker, evictionBatchSize, partitionService);
    }

    public boolean shouldUseGlobalIndex() {
        return mapConfig.getInMemoryFormat() != NATIVE || mapServiceContext.globalIndexEnabled();
    }

    protected static MemoryInfoAccessor getMemoryInfoAccessor() {
        MemoryInfoAccessor pluggedMemoryInfoAccessor = getPluggedMemoryInfoAccessor();
        return pluggedMemoryInfoAccessor != null ? pluggedMemoryInfoAccessor : new RuntimeMemoryInfoAccessor();
    }

    private static MemoryInfoAccessor getPluggedMemoryInfoAccessor() {
        String memoryInfoAccessorImpl = getProperty("hazelcast.memory.info.accessor.impl");
        if (memoryInfoAccessorImpl == null) {
            return null;
        }

        try {
            return ClassLoaderUtil.newInstance(null, memoryInfoAccessorImpl);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    // overridden in different context
    ConstructorFunction<RecordFactoryAttributes, RecordFactory> createRecordFactoryConstructor(
            final SerializationService serializationService) {
        return anyArg -> {
            switch (mapConfig.getInMemoryFormat()) {
                case BINARY:
                    return new DataRecordFactory(this, serializationService);
                case OBJECT:
                    return new ObjectRecordFactory(this, serializationService);
                default:
                    throw new IllegalArgumentException("Invalid storage format: " + mapConfig.getInMemoryFormat());
            }
        };
    }

    public void initWanReplication(NodeEngine nodeEngine) {
        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        if (wanReplicationRef == null) {
            return;
        }
        String wanReplicationRefName = wanReplicationRef.getName();

        Config config = nodeEngine.getConfig();
        if (!TRUE.equals(mapConfig.getMerkleTreeConfig().getEnabled())
                && hasPublisherWithMerkleTreeSync(config, wanReplicationRefName)) {
            throw new InvalidConfigurationException(
                    "Map " + name + " has disabled merkle trees but the WAN replication scheme "
                            + wanReplicationRefName + " has publishers that use merkle trees."
                            + " Please enable merkle trees for the map.");
        }

        WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();
        wanReplicationDelegate = wanReplicationService.getWanReplicationPublishers(wanReplicationRefName);
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        wanMergePolicy = mergePolicyProvider.getMergePolicy(wanReplicationRef.getMergePolicyClassName());
        checkMapMergePolicy(mapConfig, wanReplicationRef.getMergePolicyClassName(), mergePolicyProvider);

        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(wanReplicationRefName);
        if (wanReplicationConfig != null) {
            WanConsumerConfig wanConsumerConfig = wanReplicationConfig.getConsumerConfig();
            if (wanConsumerConfig != null) {
                persistWanReplicatedData = wanConsumerConfig.isPersistWanReplicatedData();
            }
        }
    }

    /**
     * Returns {@code true} if at least one of the WAN publishers has
     * Merkle tree consistency check configured for the given WAN
     * replication configuration
     *
     * @param config                configuration
     * @param wanReplicationRefName The name of the WAN replication
     * @return {@code true} if there is at least one publisher has Merkle
     * tree configured
     */
    private boolean hasPublisherWithMerkleTreeSync(Config config, String wanReplicationRefName) {
        WanReplicationConfig replicationConfig = config.getWanReplicationConfig(wanReplicationRefName);
        if (replicationConfig == null) {
            return false;
        }
        return replicationConfig.getBatchPublisherConfigs()
                .stream()
                .anyMatch(c -> {
                    WanSyncConfig syncConfig = c.getSyncConfig();
                    return syncConfig != null && MERKLE_TREES.equals(syncConfig.getConsistencyCheckStrategy());
                });
    }

    private PartitioningStrategy createPartitioningStrategy() {
        return mapServiceContext.getPartitioningStrategy(mapConfig.getName(), mapConfig.getPartitioningStrategyConfig());
    }

    /**
     * @return the global index, if the global index is in use or null.
     */
    public Indexes getIndexes() {
        return globalIndexes;
    }

    /**
     * @param partitionId partitionId
     */
    public Indexes getIndexes(int partitionId) {
        if (globalIndexes != null) {
            return globalIndexes;
        }
        return mapServiceContext.getPartitionContainer(partitionId).getIndexes(name);
    }

    public boolean isGlobalIndexEnabled() {
        return globalIndexes != null;
    }

    public DelegatingWanScheme getWanReplicationDelegate() {
        return wanReplicationDelegate;
    }

    public SplitBrainMergePolicy getWanMergePolicy() {
        return wanMergePolicy;
    }

    public boolean isWanReplicationEnabled() {
        return wanReplicationDelegate != null && wanMergePolicy != null;
    }

    public boolean isWanRepublishingEnabled() {
        return isWanReplicationEnabled() && mapConfig.getWanReplicationRef().isRepublishingEnabled();
    }

    public int getTotalBackupCount() {
        return getBackupCount() + getAsyncBackupCount();
    }

    public int getBackupCount() {
        return mapConfig.getBackupCount();
    }

    public int getAsyncBackupCount() {
        return mapConfig.getAsyncBackupCount();
    }

    public PartitioningStrategy getPartitioningStrategy() {
        return partitioningStrategy;
    }

    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    public MapStoreContext getMapStoreContext() {
        return mapStoreContext;
    }

    public MapConfig getMapConfig() {
        return mapConfig;
    }

    public void setMapConfig(MapConfig mapConfig) {
        this.mapConfig = mapConfig;
    }

    public EventJournalConfig getEventJournalConfig() {
        return eventJournalConfig;
    }

    public String getName() {
        return name;
    }

    public String getSplitBrainProtectionName() {
        return splitBrainProtectionName;
    }

    public Function<Object, Data> toData() {
        return toDataFunction;
    }

    public ConstructorFunction<RecordFactoryAttributes, RecordFactory> getRecordFactoryConstructor() {
        return recordFactoryConstructor;
    }

    public QueryableEntry newQueryEntry(Data key, Object value) {
        return queryEntryFactory.newEntry(key, value);
    }

    public Evictor getEvictor() {
        return evictor;
    }

    // only used for testing purposes
    public void setEvictor(Evictor evictor) {
        this.evictor = evictor;
    }

    public Extractors getExtractors() {
        return extractors;
    }

    public boolean hasInvalidationListener() {
        return invalidationListenerCounter.get() > 0;
    }

    public AtomicInteger getInvalidationListenerCounter() {
        return invalidationListenerCounter;
    }

    public InterceptorRegistry getInterceptorRegistry() {
        return interceptorRegistry;
    }

    /**
     * Callback invoked before record store and indexes are destroyed. Ensures that if map iterator observes a non-destroyed
     * state, then associated data structures are still valid.
     */
    public void onBeforeDestroy() {
        destroyed = true;
    }

    // callback called when the MapContainer is de-registered
    // from MapService and destroyed - basically on map-destroy
    public void onDestroy() {
    }

    public boolean isDestroyed() {
        return destroyed;
    }

    public boolean shouldCloneOnEntryProcessing(int partitionId) {
        return getIndexes(partitionId).haveAtLeastOneIndex()
                && OBJECT.equals(mapConfig.getInMemoryFormat());
    }

    public ObjectNamespace getObjectNamespace() {
        return objectNamespace;
    }

    public Map<String, IndexConfig> getIndexDefinitions() {
        Map<String, IndexConfig> definitions = new HashMap<>();
        if (isGlobalIndexEnabled()) {
            for (Index index : globalIndexes.getIndexes()) {
                definitions.put(index.getName(), index.getConfig());
            }
        } else {
            for (PartitionContainer container : mapServiceContext.getPartitionContainers()) {
                for (Index index : container.getIndexes(name).getIndexes()) {
                    definitions.put(index.getName(), index.getConfig());
                }
            }
        }
        return definitions;
    }

    public boolean isPersistWanReplicatedData() {
        return persistWanReplicatedData;
    }

    private class ObjectToData implements Function<Object, Data> {
        @Override
        public Data apply(Object input) {
            SerializationService ss = mapStoreContext.getSerializationService();
            return ss.toData(input, partitioningStrategy);
        }
    }

    public boolean isUseCachedDeserializedValuesEnabled(int partitionId) {
        switch (getMapConfig().getCacheDeserializedValues()) {
            case NEVER:
                return false;
            case ALWAYS:
                return true;
            default:
                //if index exists then cached value is already set -> let's use it
                return getIndexes(partitionId).haveAtLeastOneIndex();
        }
    }

    @Override
    public String toString() {
        return "MapContainer{"
                + "name='" + name + '\''
                + ", destroyed=" + destroyed
                + '}';
    }
}
