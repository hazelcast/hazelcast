package com.hazelcast.map.impl;

import com.hazelcast.map.impl.eviction.EvictionOperator;
import com.hazelcast.map.impl.eviction.ExpirationManager;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Context which is needed by a map service.
 * <p/>
 * Shared instances, configurations of all maps can be reached over this context.
 * <p/>
 * Also this context provides some support methods which are used in map operations and {@link RecordStore} implementations.
 * For example all {@link PartitionContainer} and {@link MapContainer} instances
 * can also be reached by using this interface.
 * <p/>
 * It is also responsible for providing methods which are used by lower layers of
 * Hazelcast and exposed on {@link MapService}.
 * <p/>
 *
 * @see MapManagedService
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

    void reloadOwnedPartitions();

    /**
     * Check if key belongs on partitions of the this node
     *
     * @param key key to be queried.
     * @return true if this node owns the key
     */
    boolean isOwnedKey(Data key);

    AtomicInteger getWriteBehindQueueItemCounter();

    ExpirationManager getExpirationManager();

    EvictionOperator getEvictionOperator();

    void setService(MapService mapService);

    NodeEngine getNodeEngine();

    MergePolicyProvider getMergePolicyProvider();

    MapEventPublisher getMapEventPublisher();

    MapContextQuerySupport getMapContextQuerySupport();

    LocalMapStatsProvider getLocalMapStatsProvider();
}
