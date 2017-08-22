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

package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.map.impl.eviction.EvictionChecker;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.eviction.EvictorImpl;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.nearcache.invalidation.InvalidationListener;
import com.hazelcast.map.impl.query.QueryEntryFactory;
import com.hazelcast.map.impl.record.DataRecordFactory;
import com.hazelcast.map.impl.record.ObjectRecordFactory;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.MemoryInfoAccessor;
import com.hazelcast.util.RuntimeMemoryInfoAccessor;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.map.impl.eviction.Evictor.NULL_EVICTOR;
import static com.hazelcast.map.impl.mapstore.MapStoreContextFactory.createMapStoreContext;
import static java.lang.System.getProperty;

/**
 * Map container for a map with a specific name. Contains config and supporting structures for
 * all of the maps' functionalities.
 */
public class MapContainer {

    protected final String name;
    protected final String quorumName;
    protected final MapServiceContext mapServiceContext;
    protected final Extractors extractors;
    protected final PartitioningStrategy partitioningStrategy;
    protected final MapStoreContext mapStoreContext;
    protected final SerializationService serializationService;
    protected final QueryEntryFactory queryEntryFactory;
    protected final InterceptorRegistry interceptorRegistry = new InterceptorRegistry();
    protected final IFunction<Object, Data> toDataFunction = new ObjectToData();
    protected final ConstructorFunction<Void, RecordFactory> recordFactoryConstructor;
    // on-heap indexes are global, meaning there is only one index per map, stored in the mapContainer,
    // so if globalIndexes is null it means that global index is not in use
    protected final Indexes globalIndexes;

    /**
     * Holds number of registered {@link InvalidationListener} from clients.
     */
    protected final AtomicInteger invalidationListenerCount = new AtomicInteger();

    protected final ObjectNamespace objectNamespace;

    protected WanReplicationPublisher wanReplicationPublisher;
    protected MapMergePolicy wanMergePolicy;

    protected volatile Evictor evictor;
    protected volatile MapConfig mapConfig;

    /**
     * Operations which are done in this constructor should obey the rules defined
     * in the method comment {@link com.hazelcast.spi.PostJoinAwareService#getPostJoinOperation()}
     * Otherwise undesired situations, like deadlocks, may appear.
     */
    public MapContainer(final String name, final Config config, final MapServiceContext mapServiceContext) {
        this.name = name;
        this.mapConfig = config.findMapConfig(name);
        this.mapServiceContext = mapServiceContext;
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.partitioningStrategy = createPartitioningStrategy();
        this.quorumName = mapConfig.getQuorumName();
        this.serializationService = nodeEngine.getSerializationService();
        this.recordFactoryConstructor = createRecordFactoryConstructor(serializationService);
        this.queryEntryFactory = new QueryEntryFactory(mapConfig.getCacheDeserializedValues());
        this.objectNamespace = new DistributedObjectNamespace(MapService.SERVICE_NAME, name);
        initWanReplication(nodeEngine);
        this.extractors = new Extractors(mapConfig.getMapAttributeConfigs(), config.getClassLoader());
        if (shouldUseGlobalIndex(mapConfig)) {
            this.globalIndexes = new Indexes((InternalSerializationService) serializationService,
                    mapServiceContext.getIndexProvider(mapConfig), extractors, true);
        } else {
            this.globalIndexes = null;
        }
        this.mapStoreContext = createMapStoreContext(this);
        this.mapStoreContext.start();
        initEvictor();
    }

    // this method is overridden
    public void initEvictor() {
        MapEvictionPolicy mapEvictionPolicy = mapConfig.getMapEvictionPolicy();
        if (mapEvictionPolicy == null) {
            evictor = NULL_EVICTOR;
        } else {
            MemoryInfoAccessor memoryInfoAccessor = getMemoryInfoAccessor();
            EvictionChecker evictionChecker = new EvictionChecker(memoryInfoAccessor, mapServiceContext);
            IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
            evictor = new EvictorImpl(mapEvictionPolicy, evictionChecker, partitionService);
        }
    }

    protected boolean shouldUseGlobalIndex(MapConfig mapConfig) {
        // for non-native memory populate a single global index
        return !mapConfig.getInMemoryFormat().equals(NATIVE);
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
    ConstructorFunction<Void, RecordFactory> createRecordFactoryConstructor(final SerializationService serializationService) {
        return new ConstructorFunction<Void, RecordFactory>() {
            @Override
            public RecordFactory createNew(Void notUsedArg) {
                switch (mapConfig.getInMemoryFormat()) {
                    case BINARY:
                        return new DataRecordFactory(mapConfig, serializationService, partitioningStrategy);
                    case OBJECT:
                        return new ObjectRecordFactory(mapConfig, serializationService);
                    default:
                        throw new IllegalArgumentException("Invalid storage format: " + mapConfig.getInMemoryFormat());
                }
            }
        };
    }

    public void initWanReplication(NodeEngine nodeEngine) {
        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        if (wanReplicationRef == null) {
            return;
        }
        String wanReplicationRefName = wanReplicationRef.getName();
        WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();
        wanReplicationPublisher = wanReplicationService.getWanReplicationPublisher(wanReplicationRefName);
        wanMergePolicy = mapServiceContext.getMergePolicyProvider().getMergePolicy(wanReplicationRef.getMergePolicy());
    }

    private PartitioningStrategy createPartitioningStrategy() {
        return mapServiceContext.getPartitioningStrategy(mapConfig.getName(), mapConfig.getPartitioningStrategyConfig());
    }

    /**
     * @return the global index, if the global index is in use (on-heap) or null.
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

    public WanReplicationPublisher getWanReplicationPublisher() {
        return wanReplicationPublisher;
    }

    public MapMergePolicy getWanMergePolicy() {
        return wanMergePolicy;
    }

    public boolean isWanReplicationEnabled() {
        return wanReplicationPublisher != null && wanMergePolicy != null;
    }

    public void checkWanReplicationQueues() {
        if (isWanReplicationEnabled()) {
            wanReplicationPublisher.checkWanReplicationQueues();
        }
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

    public String getName() {
        return name;
    }

    public String getQuorumName() {
        return quorumName;
    }

    public IFunction<Object, Data> toData() {
        return toDataFunction;
    }

    public ConstructorFunction<Void, RecordFactory> getRecordFactoryConstructor() {
        return recordFactoryConstructor;
    }

    public QueryableEntry newQueryEntry(Data key, Object value) {
        return queryEntryFactory.newEntry((InternalSerializationService) serializationService, key, value, extractors);
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

    public void increaseInvalidationListenerCount() {
        invalidationListenerCount.incrementAndGet();
    }

    public void decreaseInvalidationListenerCount() {
        invalidationListenerCount.decrementAndGet();
    }

    public InterceptorRegistry getInterceptorRegistry() {
        return interceptorRegistry;
    }

    // callback called when the MapContainer is de-registered from MapService and destroyed - basically on map-destroy
    public void onDestroy() {
    }

    public boolean shouldCloneOnEntryProcessing(int partitionId) {
        return getIndexes(partitionId).hasIndex() && OBJECT.equals(mapConfig.getInMemoryFormat());
    }

    public ObjectNamespace getObjectNamespace() {
        return objectNamespace;
    }

    public Map<String, Boolean> getIndexDefinitions() {
        Map<String, Boolean> definitions = new HashMap<String, Boolean>();
        if (isGlobalIndexEnabled()) {
            for (Index index : globalIndexes.getIndexes()) {
                definitions.put(index.getAttributeName(), index.isOrdered());
            }
        } else {
            for (PartitionContainer container : mapServiceContext.getPartitionContainers()) {
                for (Index index : container.getIndexes(name).getIndexes()) {
                    definitions.put(index.getAttributeName(), index.isOrdered());
                }
            }
        }
        return definitions;
    }

    @SerializableByConvention
    private class ObjectToData implements IFunction<Object, Data> {
        @Override
        public Data apply(Object input) {
            SerializationService ss = mapStoreContext.getSerializationService();
            return ss.toData(input, partitioningStrategy);
        }
    }
}
