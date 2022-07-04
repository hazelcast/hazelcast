/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import java.util.function.BiConsumer;

/**
 * Collects information about ICache usage
 */
class CacheInfoCollector implements MetricsCollector {

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        Collection<DistributedObject> objects = node.hazelcastInstance.getDistributedObjects();
        long countCacheWithWANReplication =
                objects.stream()
                       .filter(obj -> obj.getServiceName().equals(CacheService.SERVICE_NAME))
                       .map(obj -> node.getConfig().findCacheConfigOrNull(obj.getName()))
                       .filter(config -> config != null && config.getWanReplicationRef() != null)
                       .count();

        metricsConsumer.accept(
                PhoneHomeMetrics.CACHE_COUNT_WITH_WAN_REPLICATION,
                String.valueOf(countCacheWithWANReplication));
    }
}
