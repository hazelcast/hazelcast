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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.SizeEstimator;
import com.hazelcast.map.impl.operation.InvalidateNearCacheOperation;
import com.hazelcast.map.impl.operation.NearCacheKeySetInvalidationOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides near cache specific functionality.
 */
public class NearCacheProvider {

    protected final ConcurrentMap<String, NearCache> nearCacheMap = new ConcurrentHashMap<String, NearCache>();

    protected final ConstructorFunction<String, NearCache> nearCacheConstructor = new ConstructorFunction<String, NearCache>() {
        @Override
        public NearCache createNew(String mapName) {
            MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
            SizeEstimator nearCacheSizeEstimator = mapContainer.getNearCacheSizeEstimator();
            NearCacheImpl nearCache = new NearCacheImpl(mapName, nodeEngine);
            nearCache.setNearCacheSizeEstimator(nearCacheSizeEstimator);
            return nearCache;
        }
    };

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;

    public NearCacheProvider(MapServiceContext mapServiceContext, NodeEngine nodeEngine) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = nodeEngine;
    }

    public NearCache getOrCreateNearCache(String mapName) {
        return ConcurrencyUtil.getOrPutIfAbsent(nearCacheMap, mapName, nearCacheConstructor);
    }

    public void clear() {
        for (NearCache nearCache : nearCacheMap.values()) {
            nearCache.clear();
        }
        nearCacheMap.clear();
    }

    public void remove(String mapName) {
        NearCache nearCache = nearCacheMap.remove(mapName);
        if (nearCache != null) {
            nearCache.clear();
        }
    }

    public void invalidateNearCache(String mapName, Data key) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        NearCache nearCache = getOrCreateNearCache(mapName);
        nearCache.remove(key);
    }

    public void invalidateNearCache(String mapName, Collection<Data> keys) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        NearCache nearCache = getOrCreateNearCache(mapName);
        for (Data key : keys) {
            nearCache.remove(key);
        }
    }

    public void clearNearCache(String mapName) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        final NearCache nearCache = getOrCreateNearCache(mapName);
        if (nearCache != null) {
            nearCache.clear();
        }
    }

    public void invalidateAllNearCaches(String mapName, Data key) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        for (Member member : members) {
            try {
                if (member.localMember()) {
                    continue;
                }
                Operation operation = new InvalidateNearCacheOperation(mapName, key).setServiceName(MapService.SERVICE_NAME);
                nodeEngine.getOperationService().send(operation, member.getAddress());
            } catch (Throwable throwable) {
                throw new HazelcastException(throwable);
            }
        }
        // below local invalidation is for the case the data is cached before partition is owned/migrated
        invalidateNearCache(mapName, key);
    }

    public boolean isNearCacheAndInvalidationEnabled(String mapName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        return mapContainer.isNearCacheEnabled() && mapContainer.getMapConfig().getNearCacheConfig().isInvalidateOnChange();
    }

    public boolean isNearCacheEnabled(String mapName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        return mapContainer.isNearCacheEnabled();
    }

    public void invalidateAllNearCaches(String mapName, Set<Data> keys) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        if (keys == null || keys.isEmpty()) {
            return;
        }
        //send operation.
        Operation operation = new NearCacheKeySetInvalidationOperation(mapName, keys)
                .setServiceName(MapService.SERVICE_NAME);
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        for (Member member : members) {
            try {
                if (member.localMember()) {
                    continue;
                }
                nodeEngine.getOperationService().send(operation, member.getAddress());
            } catch (Throwable throwable) {
                nodeEngine.getLogger(getClass()).warning(throwable);
            }
        }
        // below local invalidation is for the case the data is cached before partition is owned/migrated
        for (final Data key : keys) {
            invalidateNearCache(mapName, key);
        }
    }

    public Object getFromNearCache(String mapName, Data key) {
        if (!isNearCacheEnabled(mapName)) {
            return null;
        }
        NearCache nearCache = getOrCreateNearCache(mapName);
        return nearCache.get(key);
    }
}

