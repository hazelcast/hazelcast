package com.hazelcast.map;

import com.hazelcast.map.eviction.EvictionOperator;
import com.hazelcast.map.eviction.ExpirationManager;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
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
public class DefaultMapServiceContext extends AbstractMapServiceContextSupport implements MapServiceContext {

    private final PartitionContainer[] partitionContainers;
    private final ConcurrentMap<String, MapContainer> mapContainers;
    private final AtomicReference<Collection<Integer>> ownedPartitions;
    private final ConstructorFunction<String, MapContainer> mapConstructor = new ConstructorFunction<String, MapContainer>() {
        public MapContainer createNew(String mapName) {
            return new MapContainer(mapName, nodeEngine.getConfig().findMapConfig(mapName),
                    getService().getMapServiceContext());
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
    private final MapEventPublisher mapEventPublisher;
    private final MapContextQuerySupport mapContextQuerySupport;
    private EvictionOperator evictionOperator;
    private MapService mapService;

    public DefaultMapServiceContext(NodeEngine nodeEngine) {
        super(nodeEngine);
        setMapServiceContext(this);
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.partitionContainers = new PartitionContainer[partitionCount];
        this.mapContainers = new ConcurrentHashMap<String, MapContainer>();
        this.ownedPartitions = new AtomicReference<Collection<Integer>>();
        this.expirationManager = new ExpirationManager(this, nodeEngine);
        this.evictionOperator = EvictionOperator.create(this);
        this.nearCacheProvider = new NearCacheProvider(this, nodeEngine);
        this.localMapStatsProvider = new LocalMapStatsProvider(this, nodeEngine);
        this.mergePolicyProvider = new MergePolicyProvider(nodeEngine);
        this.mapEventPublisher = new MapEventPublisherSupport(this);
        this.mapContextQuerySupport = new BasicMapContextQuerySupport(this);
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
            MapStoreWrapper store = mapContainer.getStore();
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
    public boolean isOwnedKey(Data key) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        Integer partitionId = partitionService.getPartitionId(key);
        try {
            Address owner = partitionService.getPartitionOwnerOrWait(partitionId);
            return nodeEngine.getThisAddress().equals(owner);
        } catch (InterruptedException e) {
            throw new HazelcastException(e);
        }
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
    public void setEvictionOperator(EvictionOperator evictionOperator) {
        this.evictionOperator = evictionOperator;
    }
}
