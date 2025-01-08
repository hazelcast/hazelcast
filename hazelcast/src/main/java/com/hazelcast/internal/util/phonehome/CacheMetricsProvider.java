/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CACHE_COUNT_WITH_WAN_REPLICATION;

/**
 * Provides information about ICache usage
 */
class CacheMetricsProvider implements MetricsProvider {

    @Override
    public void provideMetrics(Node node, MetricsCollectionContext context) {
        Collection<DistributedObject> objects = node.hazelcastInstance.getDistributedObjects();
        long countCacheWithWANReplication =
                objects.stream()
                       .filter(obj -> obj.getServiceName().equals(CacheService.SERVICE_NAME))
                       .map(obj -> node.getConfig().findCacheConfigOrNull(obj.getName()))
                       .filter(config -> config != null && config.getWanReplicationRef() != null)
                       .count();

        context.collect(CACHE_COUNT_WITH_WAN_REPLICATION, countCacheWithWANReplication);
    }
}
