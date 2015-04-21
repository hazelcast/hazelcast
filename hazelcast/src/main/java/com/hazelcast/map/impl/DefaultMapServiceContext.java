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

package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.eviction.EvictionOperator;
import com.hazelcast.map.impl.eviction.ExpirationManager;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default implementation of map service context.
 */
class DefaultMapServiceContext extends AbstractMapServiceContextSupport {

    private final PartitionContainer[] partitionContainers;
    private final ConcurrentMap<String, MapContainer> mapContainers;
    private final AtomicReference<Collection<Integer>> ownedPartitions;
    private final ConstructorFunction<String, MapContainer> mapConstructor = new ConstructorFunction<String, MapContainer>() {
        public MapContainer createNew(String mapName) {
            final MapServiceContext mapServiceContext = getService().getMapServiceContext();
            final Config config = nodeEngine.getConfig();
            final MapConfig mapConfig = config.findMapConfig(mapName);
            final MapContainer mapContainer = new MapContainer(mapName, mapConfig, mapServiceContext);
            return mapContainer;
        }
    };
    /**
     * Per node global write behind queue item counter.
     * Creating here because we want to have a counter per node.
     * This is used by owner and backups together so it should be defined
     * getting this into account.
     */
    private final AtomicInteger writeBehindQueueItemCounter = new AtomicInteger(0);
    private final ExpirationManager expirationManager;
    private final NearCacheProvider nearCacheProvider;
    private final LocalMapStatsProvider localMapStatsProvider;
    private final MergePolicyProvider mergePolicyProvider;
    private final MapContextQuerySupport mapContextQuerySupport;
    private MapEventPublisher mapEventPublisher;
    private EvictionOperator evictionOperator;
    private MapService mapService;

    public DefaultMapServiceContext(NodeEngine nodeEngine) {
        super(nodeEngine);
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.partitionContainers = new PartitionContainer[partitionCount];
        this.mapContainers = new ConcurrentHashMap<String, MapContainer>();
        this.ownedPartitions = new AtomicReference<Collection<Integer>>();
        this.expirationManager = new ExpirationManager(this, nodeEngine);
        this.evictionOperator = EvictionOperator.create(this);
        this.nearCacheProvider = new NearCacheProvider(this, nodeEngine);
        this.localMapStatsProvider = new LocalMapStatsProvider(this, nodeEngine);
        this.mergePolicyProvider = new MergePolicyProvider(nodeEngine);
        this.mapEventPublisher = createMapEventPublisherSupport();
        this.mapContextQuerySupport = new BasicMapContextQuerySupport(this);
    }

    MapEventPublisherSupport createMapEventPublisherSupport() {
        return new MapEventPublisherSupport(this);
    }

    @Override
    public MapContainer getMapContainer(String mapName) {
        return ConcurrencyUtil.getOrPutSynchronized(mapContainers, mapName, mapContainers, mapConstructor);
    }

    @Override
    public Map<String, MapContainer> getMapContainers() {
        return mapContainers;
    }

    @Override
    public PartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    @Override
    public void initPartitionsContainers() {
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        final PartitionContainer[] partitionContainers = this.partitionContainers;
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new PartitionContainer(getService(), i);
        }
    }

    @Override
    public void clearPartitionData(int partitionId) {
        final PartitionContainer container = partitionContainers[partitionId];
        if (container != null) {
            for (RecordStore mapPartition : container.getMaps().values()) {
                mapPartition.clearPartition();
            }
            container.getMaps().clear();
        }
    }

    @Override
    public String serviceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public MapService getService() {
        return mapService;
    }

    @Override
    public void clearPartitions() {
        final PartitionContainer[] containers = partitionContainers;
        for (PartitionContainer container : containers) {
            if (container != null) {
                container.clear();
            }
        }
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
        for (PartitionContainer partitionContainer : partitionContainers) {
            for (String mapName : mapContainers.keySet()) {
                RecordStore recordStore = partitionContainer.getRecordStore(mapName);
                recordStore.flush();
            }
        }
    }

    @Override
    public void destroyMap(String mapName) {
        final PartitionContainer[] containers = partitionContainers;
        for (PartitionContainer container : containers) {
            if (container != null) {
                container.destroyMap(mapName);
            }
        }
    }

    @Override
    public void reset() {
        clearPartitions();
        getNearCacheProvider().clear();
    }

    @Override
    public NearCacheProvider getNearCacheProvider() {
        return nearCacheProvider;
    }

    @Override
    public RecordStore getRecordStore(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getRecordStore(mapName);
    }

    @Override
    public RecordStore getExistingRecordStore(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getExistingRecordStore(mapName);
    }

    @Override
    public Collection<Integer> getOwnedPartitions() {
        Collection<Integer> partitions = ownedPartitions.get();
        if (partitions == null) {
            reloadOwnedPartitions();
            partitions = ownedPartitions.get();
        }
        return partitions;
    }

    @Override
    public void reloadOwnedPartitions() {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        Collection<Integer> partitions = partitionService.getMemberPartitions(nodeEngine.getThisAddress());
        if (partitions == null) {
            partitions = Collections.emptySet();
        }
        ownedPartitions.set(Collections.unmodifiableSet(new LinkedHashSet<Integer>(partitions)));
    }

    @Override
    public AtomicInteger getWriteBehindQueueItemCounter() {
        return writeBehindQueueItemCounter;
    }

    @Override
    public ExpirationManager getExpirationManager() {
        return expirationManager;
    }

    @Override
    public EvictionOperator getEvictionOperator() {
        return evictionOperator;
    }

    @Override
    public void setService(MapService mapService) {
        this.mapService = mapService;
    }

    @Override
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public MergePolicyProvider getMergePolicyProvider() {
        return mergePolicyProvider;
    }

    @Override
    public MapEventPublisher getMapEventPublisher() {
        return mapEventPublisher;
    }

    @Override
    public MapContextQuerySupport getMapContextQuerySupport() {
        return mapContextQuerySupport;
    }

    @Override
    public LocalMapStatsProvider getLocalMapStatsProvider() {
        return localMapStatsProvider;
    }

    /**
     * Used for testing purposes.
     */
    @Override
    public void setEvictionOperator(EvictionOperator evictionOperator) {
        this.evictionOperator = evictionOperator;
    }
}
