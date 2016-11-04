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

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.impl.DefaultNearCacheManager;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapManagedService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.invalidation.BatchInvalidator;
import com.hazelcast.map.impl.nearcache.invalidation.NearCacheInvalidator;
import com.hazelcast.map.impl.nearcache.invalidation.NonStopInvalidator;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.properties.HazelcastProperties;

import static com.hazelcast.map.impl.nearcache.StaleReadPreventerNearCacheWrapper.asStaleReadPreventerNearCache;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;

public class MapNearCacheManager extends DefaultNearCacheManager {

    protected final NodeEngine nodeEngine;
    protected final MapServiceContext mapServiceContext;
    protected final NearCacheInvalidator nearCacheInvalidator;
    protected final int partitionCount;

    public MapNearCacheManager(MapServiceContext mapServiceContext) {
        super(mapServiceContext.getNodeEngine().getSerializationService(),
                mapServiceContext.getNodeEngine().getExecutionService(), null);
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.mapServiceContext = mapServiceContext;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.nearCacheInvalidator = isBatchingEnabled() ? new BatchInvalidator(nodeEngine) : new NonStopInvalidator(nodeEngine);
    }

    private boolean isBatchingEnabled() {
        HazelcastProperties hazelcastProperties = nodeEngine.getProperties();
        int batchSize = hazelcastProperties.getInteger(MAP_INVALIDATION_MESSAGE_BATCH_SIZE);
        return hazelcastProperties.getBoolean(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED) && batchSize > 1;
    }

    public <K, V> NearCache<K, V> getOrCreateNearCache(String mapName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        NearCacheConfig nearCacheConfig = mapContainer.getMapConfig().getNearCacheConfig();
        return getOrCreateNearCache(mapName, nearCacheConfig);
    }

    @Override
    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig) {
        NearCache nearCache = super.createNearCache(name, nearCacheConfig);
        return (NearCache<K, V>) asStaleReadPreventerNearCache(nearCache, partitionCount);
    }

    /**
     * @see MapManagedService#reset()
     */
    public void reset() {
        clearAllNearCaches();
        nearCacheInvalidator.reset();
    }

    /**
     * @see MapManagedService#shutdown(boolean)
     */
    public void shutdown() {
        destroyAllNearCaches();
        nearCacheInvalidator.shutdown();
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
        nearCacheInvalidator.destroy(mapName, uuid);
        return true;
    }

    public Object getFromNearCache(String mapName, Data key) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        if (!mapContainer.hasInvalidationListener()) {
            return null;
        }

        NearCache<Data, Object> nearCache = getOrCreateNearCache(mapName);
        return nearCache.get(key);
    }

    public NearCacheInvalidator getNearCacheInvalidator() {
        return nearCacheInvalidator;
    }


}
