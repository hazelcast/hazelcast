package com.hazelcast.cache.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.operation.UpdateMapConfigOperation;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;

public class HazelcastConfigurator {
    static MapConfig updateMapConfig(String mapName, Config config, MyCustomSettings settings) {
        System.out.println("convertAndStoreMapConfig for " + mapName);
        MapConfig newMapConfig = new MapConfig(config.findMapConfig(mapName));
        newMapConfig.getMaxSizeConfig().setSize(settings.getCapacity());
        newMapConfig.setName(mapName);

        NearCacheConfig nearCacheConfig = newMapConfig.getNearCacheConfig();
        if (nearCacheConfig == null) {
            nearCacheConfig = new NearCacheConfig();
        }
        nearCacheConfig.getEvictionConfig().setSize(settings.getCapacity());
        newMapConfig.setNearCacheConfig(nearCacheConfig);
        return newMapConfig;
    }

    @SuppressWarnings("unchecked")
    static void configureMap(HazelcastInstance hazelcast, String mapName, MyCustomSettings settings) {
        MapConfig mapConfig = updateMapConfig(mapName, hazelcast.getConfig(), settings);

        UpdateMapConfigOperation operation = new UpdateMapConfigOperation(mapName, mapConfig);
        MapProxyImpl proxy = hazelcast.getDistributedObject(MapService.SERVICE_NAME, mapName);
        operation.setService(proxy.getService());
        try {
            operation.run();
        } catch (Exception e) {
            System.out.println("Error updating map settings!!!");
        }
    }
}
