/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.impl.DefaultNearCacheManager;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataFetcher;
import com.hazelcast.internal.nearcache.impl.invalidation.MinimalPartitionService;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapManagedService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.invalidation.BatchInvalidator;
import com.hazelcast.map.impl.nearcache.invalidation.Invalidator;
import com.hazelcast.map.impl.nearcache.invalidation.MemberMapMetaDataFetcher;
import com.hazelcast.map.impl.nearcache.invalidation.NonStopInvalidator;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.properties.HazelcastProperties;

import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;

public class MapNearCacheManager extends DefaultNearCacheManager {

    protected final int partitionCount;
    protected final NodeEngine nodeEngine;
    protected final MapServiceContext mapServiceContext;
    protected final Invalidator invalidator;
    protected final RepairingTask repairingTask;
    protected final MinimalPartitionService partitionService;

    public MapNearCacheManager(MapServiceContext mapServiceContext) {
        super(mapServiceContext.getNodeEngine().getSerializationService(),
                mapServiceContext.getNodeEngine().getExecutionService(), null);
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.partitionService = new MemberMinimalPartitionService(nodeEngine.getPartitionService());
        this.mapServiceContext = mapServiceContext;
        this.partitionCount = partitionService.getPartitionCount();
        this.invalidator = isBatchingEnabled()
                ? new BatchInvalidator(mapServiceContext) : new NonStopInvalidator(mapServiceContext);
        this.repairingTask = createRepairingInvalidationTask();
    }

    private RepairingTask createRepairingInvalidationTask() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        ClusterService clusterService = nodeEngine.getClusterService();
        OperationService operationService = nodeEngine.getOperationService();
        HazelcastProperties properties = nodeEngine.getProperties();
        ILogger logger = nodeEngine.getLogger(RepairingTask.class);

        MetaDataFetcher metaDataFetcher = new MemberMapMetaDataFetcher(clusterService, operationService, logger);
        String localUuid = nodeEngine.getLocalMember().getUuid();
        return new RepairingTask(metaDataFetcher, executionService, partitionService, properties, localUuid, logger);
    }

    private boolean isBatchingEnabled() {
        HazelcastProperties hazelcastProperties = nodeEngine.getProperties();
        int batchSize = hazelcastProperties.getInteger(MAP_INVALIDATION_MESSAGE_BATCH_SIZE);
        return hazelcastProperties.getBoolean(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED) && batchSize > 1;
    }

    /**
     * @see MapManagedService#reset()
     */
    public void reset() {
        clearAllNearCaches();
        invalidator.reset();
    }

    /**
     * @see MapManagedService#shutdown(boolean)
     */
    public void shutdown() {
        destroyAllNearCaches();
        invalidator.shutdown();
    }

    /**
     * @see com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)
     */
    @Override
    public boolean destroyNearCache(String mapName) {
        if (!super.destroyNearCache(mapName)) {
            return false;
        }

        String uuid = nodeEngine.getLocalMember().getUuid();
        invalidator.destroy(mapName, uuid);
        return true;
    }

    public Object getFromNearCache(String mapName, Data key) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        if (!mapContainer.hasInvalidationListener()) {
            return null;
        }

        NearCacheConfig nearCacheConfig = mapContainer.getMapConfig().getNearCacheConfig();
        NearCache<Data, Object> nearCache = getOrCreateNearCache(mapName, nearCacheConfig);
        return nearCache.get(key);
    }

    public Invalidator getInvalidator() {
        return invalidator;
    }

    public InvalidationHandler newRepairingHandler(String name, NearCache nearCache) {
        return repairingTask.registerAndGetHandler(name, nearCache);
    }

    // only used in tests.
    public RepairingHandler getRepairingHandlerOrNull(String name) {
        return repairingTask.getHandlerOrNull(name);
    }

    public void deregisterRepairingHandler(String name) {
        repairingTask.deregisterHandler(name);
    }
}
