package com.hazelcast.map.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.MemberImpl;
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

    private final ConcurrentMap<String, NearCache> nearCacheMap = new ConcurrentHashMap<String, NearCache>();

    private final ConstructorFunction<String, NearCache> nearCacheConstructor = new ConstructorFunction<String, NearCache>() {
        public NearCache createNew(String mapName) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
            final SizeEstimator nearCacheSizeEstimator = mapContainer.getNearCacheSizeEstimator();
            final NearCache nearCache = new NearCache(mapName, nodeEngine);
            nearCache.setNearCacheSizeEstimator(nearCacheSizeEstimator);
            return nearCache;
        }
    };

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    public NearCacheProvider(MapServiceContext mapServiceContext, NodeEngine nodeEngine) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = nodeEngine;
    }

    public NearCache getNearCache(String mapName) {
        return ConcurrencyUtil.getOrPutIfAbsent(nearCacheMap, mapName, nearCacheConstructor);
    }

    public void clear() {
        for (NearCache nearCache : nearCacheMap.values()) {
            nearCache.clear();
        }
        nearCacheMap.clear();
    }

    public void remove(String mapName) {
        final NearCache nearCache = nearCacheMap.remove(mapName);
        if (nearCache != null) {
            nearCache.clear();
        }
    }

    // this operation returns the given value in near-cache memory format (data or object)
    // if near-cache is not enabled, it returns null
    public Object putNearCache(String mapName, Data key, Data value) {
        // todo assert near-cache is enabled might be better
        if (!isNearCacheEnabled(mapName)) {
            return null;
        }
        NearCache nearCache = getNearCache(mapName);
        return nearCache.put(key, value);
    }

    public void invalidateNearCache(String mapName, Data key) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        NearCache nearCache = getNearCache(mapName);
        nearCache.invalidate(key);
    }

    public void invalidateNearCache(String mapName, Collection<Data> keys) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        NearCache nearCache = getNearCache(mapName);
        nearCache.invalidate(keys);
    }

    public void clearNearCache(String mapName) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        final NearCache nearCache = getNearCache(mapName);
        if (nearCache != null) {
            nearCache.clear();
        }
    }

    public void invalidateAllNearCaches(String mapName, Data key) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
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
        final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        return mapContainer.isNearCacheEnabled()
                && mapContainer.getMapConfig().getNearCacheConfig().isInvalidateOnChange();
    }

    public boolean isNearCacheEnabled(String mapName) {
        final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
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
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
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
        NearCache nearCache = getNearCache(mapName);
        return nearCache.get(key);
    }
}

