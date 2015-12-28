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

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.eviction.ExpirationManager;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.query.MapQueryEngine;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

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
public interface MapServiceContext extends MapServiceContextInterceptorSupport, MapServiceContextEventListenerSupport {

    Object toObject(Object data);

    Data toData(Object object, PartitioningStrategy partitionStrategy);

    Data toData(Object object);

    MapContainer getMapContainer(String mapName);

    Map<String, MapContainer> getMapContainers();

    PartitionContainer getPartitionContainer(int partitionId);

    void initPartitionsContainers();

    void clearPartitionData(int partitionId);

    MapService getService();

    /**
     * Clears all partition based data allocated by MapService.
     *
     * @param onShutdown true if {@code clearPartitions} is called during MapService shutdown,
     *                   false otherwise.
     */
    void clearPartitions(boolean onShutdown);

    void destroyMapStores();

    void flushMaps();

    void destroyMap(String mapName);

    void reset();

    /**
     * Releases internal resources solely managed by Hazelcast. This method is
     * called when MapService is shutting down.
     */
    void shutdown();

    NearCacheProvider getNearCacheProvider();

    RecordStore createRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader);

    RecordStore getRecordStore(int partitionId, String mapName);

    RecordStore getExistingRecordStore(int partitionId, String mapName);

    Collection<Integer> getOwnedPartitions();

    void reloadOwnedPartitions();

    AtomicInteger getWriteBehindQueueItemCounter();

    ExpirationManager getExpirationManager();

    void setService(MapService mapService);

    NodeEngine getNodeEngine();

    MergePolicyProvider getMergePolicyProvider();

    MapEventPublisher getMapEventPublisher();

    MapQueryEngine getMapQueryEngine(String name);

    LocalMapStatsProvider getLocalMapStatsProvider();

    MapOperationProvider getMapOperationProvider(String name);

    Extractors getExtractors(String mapName);

    void incrementOperationStats(long startTime, LocalMapStatsImpl localMapStats, String mapName, Operation operation);
}
