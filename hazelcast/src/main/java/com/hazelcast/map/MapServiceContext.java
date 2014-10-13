package com.hazelcast.map;

import com.hazelcast.map.eviction.ExpirationManager;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Context which is needed by a map service.
 * <p/>
 * Shared instances, configurations of all maps can be reached over this context.
 * <p/>
 * Also this context provides some support methods which are used in map operations and {@link RecordStore} implementations.
 * For example all {@link com.hazelcast.map.PartitionContainer} and {@link com.hazelcast.map.MapContainer} instances
 * can also be reached by using this interface.
 * <p/>
 * It is also responsible for providing methods which are used by lower layers of
 * Hazelcast and exposed on {@link com.hazelcast.map.MapService}.
 * <p/>
 *
 * @see com.hazelcast.map.MapManagedService
 */
public interface MapServiceContext extends MapServiceContextSupport,
        MapServiceContextInterceptorSupport, MapServiceContextEventListenerSupport {

    MapContainer getMapContainer(String mapName);

    Map<String, MapContainer> getMapContainers();

    PartitionContainer getPartitionContainer(int partitionId);

    void initPartitionsContainers();

    void clearPartitionData(int partitionId);

    String serviceName();

    MapService getService();

    void clearPartitions();

    void destroyMapStores();

    void flushMaps();

    void destroyMap(String mapName);

    void reset();

    NearCacheProvider getNearCacheProvider();

    RecordStore getRecordStore(int partitionId, String mapName);

    RecordStore getExistingRecordStore(int partitionId, String mapName);

    Collection<Integer> getOwnedPartitions();

    AtomicReference<Collection<Integer>> ownedPartitions();

    /**
     * Check if key belongs on partitions of the this node
     * @param key
     * @return true if this node owns the key
     */
    boolean isOwnedKey(Data key);

    Set<Integer> getMemberPartitions();

    AtomicInteger getWriteBehindQueueItemCounter();

    ExpirationManager getExpirationManager();

    void setService(MapService mapService);

    NodeEngine getNodeEngine();

    MergePolicyProvider getMergePolicyProvider();

    MapEventPublisher getMapEventPublisher();

    MapContextQuerySupport getMapContextQuerySupport();

    LocalMapStatsProvider getLocalMapStatsProvider();
}
