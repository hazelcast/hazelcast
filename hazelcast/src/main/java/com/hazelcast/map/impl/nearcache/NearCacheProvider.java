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
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapManagedService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.nearcache.StaleReadPreventerNearCacheWrapper.wrapAsStaleReadPreventerNearCache;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;

/**
 * Provides Near Cache specific functionality.
 */
public class NearCacheProvider {

    protected final ConcurrentMap<String, NearCache<Data, Object>> nearCacheMap
            = new ConcurrentHashMap<String, NearCache<Data, Object>>();

    protected final ConstructorFunction<String, NearCache<Data, Object>> nearCacheConstructor
            = new ConstructorFunction<String, NearCache<Data, Object>>() {
        @Override
        public NearCache<Data, Object> createNew(String mapName) {
            MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
            NearCache<Data, Object> nearCache = new NearCacheImpl(mapName, nodeEngine, mapContainer.getNearCacheSizeEstimator());

            int partitionCount = mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount();
            return wrapAsStaleReadPreventerNearCache(nearCache, partitionCount);
        }
    };

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final NearCacheInvalidator nearCacheInvalidator;

    public NearCacheProvider(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.nearCacheInvalidator = createNearCacheInvalidator(mapServiceContext);
    }

    protected NearCacheInvalidator createNearCacheInvalidator(MapServiceContext mapServiceContext) {
        return isBatchingEnabled()
                ? new BatchInvalidator(mapServiceContext, this)
                : new NonStopInvalidator(mapServiceContext, this);
    }

    private boolean isBatchingEnabled() {
        HazelcastProperties hazelcastProperties = nodeEngine.getProperties();
        int batchSize = hazelcastProperties.getInteger(MAP_INVALIDATION_MESSAGE_BATCH_SIZE);
        return hazelcastProperties.getBoolean(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED) && batchSize > 1;
    }

    public NearCache<Data, Object> getOrCreateNearCache(String mapName) {
        return ConcurrencyUtil.getOrPutIfAbsent(nearCacheMap, mapName, nearCacheConstructor);
    }

    NearCache getOrNullNearCache(String mapName) {
        return nearCacheMap.get(mapName);
    }

    /**
     * @see MapManagedService#reset()
     */
    public void reset() {
        Collection<NearCache<Data, Object>> nearCaches = nearCacheMap.values();
        for (NearCache nearCache : nearCaches) {
            nearCache.clear();
        }
        nearCacheMap.clear();
        nearCacheInvalidator.reset();
    }

    /**
     * @see MapManagedService#shutdown(boolean)
     */
    public void shutdown() {
        Collection<NearCache<Data, Object>> nearCaches = nearCacheMap.values();
        for (NearCache nearCache : nearCaches) {
            nearCache.destroy();
        }
        nearCacheMap.clear();
        nearCacheInvalidator.shutdown();
    }

    /**
     * @see com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)
     */
    public void destroyNearCache(String mapName) {
        NearCache nearCache = nearCacheMap.remove(mapName);
        if (nearCache != null) {
            nearCache.destroy();
        }

        nearCacheInvalidator.destroy(mapName);
    }

    public Object getFromNearCache(String mapName, Data key) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        if (!mapContainer.hasMemberNearCache()) {
            return null;
        }
        NearCache<Data, Object> nearCache = getOrCreateNearCache(mapName);
        return nearCache.get(key);
    }

    public NearCacheInvalidator getNearCacheInvalidator() {
        return nearCacheInvalidator;
    }
}
