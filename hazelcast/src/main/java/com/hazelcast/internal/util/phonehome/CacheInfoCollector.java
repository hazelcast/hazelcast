package com.hazelcast.internal.util.phonehome;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.impl.Node;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class CacheInfoCollector implements MetricsCollector {

    Collection<DistributedObject> caches;

    @Override
    public Map<String, String> computeMetrics(Node hazelcastNode) {

        Collection<DistributedObject> distributedObjects = hazelcastNode.hazelcastInstance.getDistributedObjects();
        caches = distributedObjects.stream().filter(distributedObject -> distributedObject.getServiceName().
                equals(CacheService.SERVICE_NAME)).collect(toList());
        Map<String, String> cacheInfo = new HashMap<>();
        cacheInfo.put("cawact", String.valueOf(countCacheWithWANReplication(hazelcastNode)));
        return cacheInfo;
    }

    private long countCacheWithWANReplication(Node node) {
        return caches.stream().filter(distributedObject -> {
            CacheSimpleConfig config = node.getConfig().findCacheConfigOrNull(distributedObject.getName());
            if (config != null) {
                return config.getWanReplicationRef() != null;
            }
            return false;
        }).count();
    }
}
