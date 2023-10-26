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

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.PostJoinAwareService;
import com.hazelcast.internal.util.Clock;
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
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.wan.MapWanContext;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider.getEvictionPolicyComparator;
import static com.hazelcast.map.impl.eviction.Evictor.NULL_EVICTOR;
import static com.hazelcast.map.impl.mapstore.MapStoreContextFactory.createMapStoreContext;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_EVICTION_BATCH_SIZE;
import static java.lang.System.getProperty;

/**
 * Map container for a map with a specific name. Contains config and
 * supporting structures for all of the maps' functionalities.
 */
@SuppressWarnings({"WeakerAccess", "checkstyle:classfanoutcomplexity"})
public class MapContainer {
    static final int GLOBAL_INDEX_NOOP_PARTITION_ID = -1;

    protected final String name;
    protected final String splitBrainProtectionName;
    // on-heap indexes are global, meaning there is only one index per map,
    // stored in the mapContainer, so if globalIndexes is null it means that
    // global index is not in use
    protected final Extractors extractors;
    protected final MapStoreContext mapStoreContext;
    protected final ObjectNamespace objectNamespace;
    protected final IndexRegistry globalIndexRegistry;
    protected final MapServiceContext mapServiceContext;
    protected final QueryEntryFactory queryEntryFactory;
    protected final EventJournalConfig eventJournalConfig;
    protected final PartitioningStrategy partitioningStrategy;
    protected final InternalSerializationService serializationService;
    protected final Function<Object, Data> toDataFunction = new ObjectToData();
    protected final InterceptorRegistry interceptorRegistry = new InterceptorRegistry();
    protected final ConcurrentMap<Integer, IndexRegistry> partitionedIndexRegistry = new ConcurrentHashMap<>();

    /**
     * Holds number of registered {@link InvalidationListener} from clients.
     */
    protected final AtomicInteger invalidationListenerCount = new AtomicInteger();
    protected final AtomicLong lastInvalidMergePolicyCheckTime = new AtomicLong();


    protected volatile MapConfig mapConfig;
    private volatile Evictor evictor;

    private final MapWanContext wanContext;

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
        this.objectNamespace = MapService.getObjectNamespace(name);
        this.extractors = Extractors.newBuilder(serializationService)
                .setAttributeConfigs(mapConfig.getAttributeConfigs())
                .setClassLoader(nodeEngine.getConfigClassLoader())
                .build();
        this.queryEntryFactory = new QueryEntryFactory(mapConfig.getCacheDeserializedValues(),
                serializationService, extractors);
        this.globalIndexRegistry = shouldUseGlobalIndex()
                ? createIndexRegistry(true, GLOBAL_INDEX_NOOP_PARTITION_ID) : null;
        this.mapStoreContext = createMapStoreContext(this);
        this.wanContext = new MapWanContext(this);
    }

    public void init() {
        initEvictor();
        mapStoreContext.start();
        wanContext.start();
    }

    /**
     * @param global      set {@code true} to create global indexes, otherwise set
     *                    {@code false} to have partitioned indexes
     * @param partitionId the partition ID the index is created on. {@code -1}
     *                    for global indexes.
     * @return a new IndexRegistry object
     */
    public IndexRegistry createIndexRegistry(boolean global, int partitionId) {
        int partitionCount = mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount();

        Node node = ((NodeEngineImpl) mapServiceContext.getNodeEngine()).getNode();
        IndexRegistry newIndexRegistry = IndexRegistry.newBuilder(node, getName(),
                        serializationService, mapServiceContext.getIndexCopyBehavior(),
                        mapConfig.getInMemoryFormat())
                .global(global)
                .extractors(extractors)
                .statsEnabled(mapConfig.isStatisticsEnabled())
                .indexProvider(mapServiceContext.getIndexProvider(mapConfig))
                .usesCachedQueryableEntries(mapConfig.getCacheDeserializedValues() != CacheDeserializedValues.NEVER)
                .partitionCount(partitionCount)
                .partitionId(partitionId)
                .resultFilterFactory(new IndexResultFilterFactory())
                .build();

        // if global index, return registry
        if (partitionId == GLOBAL_INDEX_NOOP_PARTITION_ID) {
            return newIndexRegistry;
        } else {
            // if partitioned index, first register it to
            // partitionedIndexRegistry then return registry
            IndexRegistry currentIndexRegistry
                    = partitionedIndexRegistry.putIfAbsent(partitionId, newIndexRegistry);
            return currentIndexRegistry == null ? newIndexRegistry : currentIndexRegistry;
        }
    }

    // -------------------------------------------------------------------------------------------------------------
    // IMPORTANT: never use directly! use MapContainer.getIndex() instead.
    // There are cases where a global index is used. In this case, the global-index is stored in the MapContainer.
    // By using this method in the context of global index an exception will be thrown.
    // -------------------------------------------------------------------------------------------------------------
    IndexRegistry getOrCreatePartitionedIndexRegistry(int partitionId) {
        if (isGlobalIndexEnabled()) {
            throw new IllegalStateException("Can't use a partitioned-index in the context of a global-index.");
        }

        IndexRegistry existing = partitionedIndexRegistry.get(partitionId);
        return existing != null ? existing : createIndexRegistry(false, partitionId);
    }

    public AtomicLong getLastInvalidMergePolicyCheckTime() {
        return lastInvalidMergePolicyCheckTime;
    }

    private class IndexResultFilterFactory implements Supplier<Predicate<QueryableEntry>> {

        @Override
        public Predicate<QueryableEntry> get() {
            return new Predicate<>() {
                private final long nowInMillis = Clock.currentTimeMillis();

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

        if (!getOrCreateIndexRegistry(partitionId).isGlobal()) {
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

    private PartitioningStrategy createPartitioningStrategy() {
        return mapServiceContext.getPartitioningStrategy(
                mapConfig.getName(),
                mapConfig.getPartitioningStrategyConfig(),
                mapConfig.getPartitioningAttributeConfigs()
        );
    }

    /**
     * Used to get index registry of one
     * of global or partitioned indexes.
     *
     * @param partitionId partitionId
     * @return by default always returns global-index
     * registry otherwise return partitioned-index registry
     */
    public IndexRegistry getOrCreateIndexRegistry(int partitionId) {
        if (globalIndexRegistry != null) {
            return globalIndexRegistry;
        }

        return getOrCreatePartitionedIndexRegistry(partitionId);
    }

    /**
     * @return the global index, if the global index is in use or null.
     */
    public IndexRegistry getGlobalIndexRegistry() {
        return globalIndexRegistry;
    }

    @Nullable
    public IndexRegistry getOrNullPartitionedIndexRegistry(int partitionId) {
        return partitionedIndexRegistry.get(partitionId);
    }

    // Only used for testing
    public ConcurrentMap<Integer, IndexRegistry> getPartitionedIndexRegistry() {
        return partitionedIndexRegistry;
    }

    // Only used for testing
    public boolean isEmptyIndexRegistry() {
        if (globalIndexRegistry != null) {
            return globalIndexRegistry.getIndexes().length == 0;
        }
        return partitionedIndexRegistry.isEmpty();
    }

    public boolean isGlobalIndexEnabled() {
        return globalIndexRegistry != null;
    }

    public MapWanContext getWanContext() {
        return wanContext;
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
        return invalidationListenerCount.get() > 0;
    }

    public AtomicInteger getInvalidationListenerCounter() {
        return invalidationListenerCount;
    }

    public void increaseInvalidationListenerCount() {
        invalidationListenerCount.incrementAndGet();
    }

    public void decreaseInvalidationListenerCount() {
        invalidationListenerCount.decrementAndGet();
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
        return getOrCreateIndexRegistry(partitionId).haveAtLeastOneIndex()
                && OBJECT.equals(mapConfig.getInMemoryFormat());
    }

    public ObjectNamespace getObjectNamespace() {
        return objectNamespace;
    }

    public Map<String, IndexConfig> getIndexDefinitions() {
        return isGlobalIndexEnabled()
                ? getGlobalIndexDefinitions()
                : getPartitionedIndexDefinitions();
    }

    private Map<String, IndexConfig> getGlobalIndexDefinitions() {
        Map<String, IndexConfig> definitions = new HashMap<>();
        InternalIndex[] indexes = globalIndexRegistry.getIndexes();
        for (int i = 0; i < indexes.length; i++) {
            definitions.put(indexes[i].getName(), indexes[i].getConfig());
        }
        return definitions;
    }

    private Map<String, IndexConfig> getPartitionedIndexDefinitions() {
        Map<String, IndexConfig> definitions = new HashMap<>();
        int partitionCount = mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            IndexRegistry indexRegistry = getOrNullPartitionedIndexRegistry(i);
            if (indexRegistry == null) {
                continue;
            }

            InternalIndex[] indexes = indexRegistry.getIndexes();
            for (int j = 0; j < indexes.length; j++) {
                definitions.put(indexes[j].getName(), indexes[j].getConfig());
            }
        }
        return definitions;
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
                return getOrCreateIndexRegistry(partitionId).haveAtLeastOneIndex();
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
