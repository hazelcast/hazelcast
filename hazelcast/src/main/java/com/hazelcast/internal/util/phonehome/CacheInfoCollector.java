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
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.impl.Node;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class CacheInfoCollector implements MetricsCollector {

    @Override
    public Map<PhoneHomeMetrics, String> computeMetrics(Node hazelcastNode) {

        Map<PhoneHomeMetrics, String> cacheInfo = new HashMap<>(1);
        Collection<DistributedObject> distributedObjects = hazelcastNode.hazelcastInstance.getDistributedObjects();

        long countCacheWithWANReplication = distributedObjects.stream()
                .filter(distributedObject -> distributedObject.getServiceName().equals(CacheService.SERVICE_NAME))
                .map(distributedObject -> hazelcastNode.getConfig().findCacheConfigOrNull(distributedObject.getName()))
                .filter(cacheConfig -> cacheConfig != null && cacheConfig.getWanReplicationRef() != null)
                .count();
        cacheInfo.put(PhoneHomeMetrics.CACHE_COUNT_WITH_WAN_REPLICATION, String.valueOf(countCacheWithWANReplication));

        return cacheInfo;
    }
}
