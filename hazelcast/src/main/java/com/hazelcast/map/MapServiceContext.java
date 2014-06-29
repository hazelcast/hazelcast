package com.hazelcast.map;

import com.hazelcast.map.eviction.ExpirationManager;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.spi.NodeEngine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public interface MapServiceContext extends MapServiceContextSupport,
        MapServiceContextInterceptorSupport, MapServiceContextEventListenerSupport {

    MapContainer getMapContainer(String mapName);

    Map<String, MapContainer> getMapContainers();

    PartitionContainer getPartitionContainer(int partitionId);

    PartitionContainer[] getPartitionContainers();

    void clearPartitionData(int partitionId);

    String serviceName();

    MapService getService();

    void clearPartitions();

    void destroyMapStores();

    void flushMaps();

    void destroyMap(String mapName);

    LocalMapStatsImpl getLocalMapStatsImpl(String mapName);

    NearCacheProvider getNearCacheProvider();

    RecordStore getRecordStore(int partitionId, String mapName);

    RecordStore getExistingRecordStore(int partitionId, String mapName);

    List<Integer> getOwnedPartitions();

    AtomicReference<List<Integer>> ownedPartitions();

    List<Integer> getMemberPartitions();

    AtomicInteger getWriteBehindQueueItemCounter();

    ExpirationManager getExpirationManager();

    void setService(MapService mapService);

    NodeEngine getNodeEngine();

    MergePolicyProvider getMergePolicyProvider();

    MapEventPublisher getMapEventPublisher();

    MapContextQuerySupport getMapContextQuerySupport();

    LocalMapStatsProvider getLocalMapStatsProvider();
}
