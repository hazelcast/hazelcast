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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.NearCacheBatchInvalidationOperation;
import com.hazelcast.map.impl.operation.NearCacheSingleInvalidationOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.util.List;

import static com.hazelcast.util.CollectionUtil.isEmpty;


/**
 * Contains common functionality of a {@code NearCacheInvalidator}
 */
public abstract class AbstractNearCacheInvalidator implements NearCacheInvalidator {

    protected final EventService eventService;
    protected final OperationService operationService;
    protected final ClusterService clusterService;
    protected final MapServiceContext mapServiceContext;
    protected final NearCacheProvider nearCacheProvider;
    protected final NodeEngine nodeEngine;

    public AbstractNearCacheInvalidator(MapServiceContext mapServiceContext, NearCacheProvider nearCacheProvider) {
        this.mapServiceContext = mapServiceContext;
        this.nearCacheProvider = nearCacheProvider;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.eventService = nodeEngine.getEventService();
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
    }


    public void invalidateLocal(String mapName, Data key, List<Data> keys) {
        if (!isMemberNearCacheInvalidationEnabled(mapName)) {
            return;
        }

        NearCache nearCache = nearCacheProvider.getOrNullNearCache(mapName);
        if (nearCache != null) {
            if (key != null) {
                nearCache.remove(key);
            } else if (!isEmpty(keys)) {
                for (Data data : keys) {
                    nearCache.remove(data);
                }
            }
        }
    }

    public void clearLocal(String mapName) {
        if (!isMemberNearCacheInvalidationEnabled(mapName)) {
            return;
        }

        NearCache nearCache = nearCacheProvider.getOrNullNearCache(mapName);
        if (nearCache != null) {
            nearCache.clear();
        }
    }

    protected boolean isMemberNearCacheInvalidationEnabled(String mapName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        return mapContainer.isMemberNearCacheInvalidationEnabled();
    }

    protected boolean hasInvalidationListener(String mapName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        return mapContainer.hasInvalidationListener();
    }


    protected Data toHeapData(Data key) {
        return mapServiceContext.toData(key);
    }


    public static Object getOrderKey(String mapName, Invalidation invalidation) {
        if (invalidation instanceof SingleNearCacheInvalidation) {
            return ((SingleNearCacheInvalidation) invalidation).getKey();
        } else {
            return mapName;
        }
    }

    public static MapOperation createSingleOrBatchInvalidationOperation(String mapName, Data key, List<Data> keys) {

        if (key != null) {
            return new NearCacheSingleInvalidationOperation(mapName, key);
        }

        if (keys != null) {
            return new NearCacheBatchInvalidationOperation(mapName, keys);
        }

        throw new IllegalArgumentException("One of key or keys should be provided for invalidation");
    }

}
