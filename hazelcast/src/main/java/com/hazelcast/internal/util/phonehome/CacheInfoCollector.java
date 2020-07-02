/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.impl.Node;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toList;

class CacheInfoCollector implements MetricsCollector {

    private Collection<DistributedObject> caches;

    @Override
    public Map<String, String> computeMetrics(Node hazelcastNode) {

        Collection<DistributedObject> distributedObjects = hazelcastNode.hazelcastInstance.getDistributedObjects();
        caches = distributedObjects.stream().filter(distributedObject -> distributedObject.getServiceName().
                equals(CacheService.SERVICE_NAME)).collect(toList());
        Map<String, String> cacheInfo = new HashMap<>(1);
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
