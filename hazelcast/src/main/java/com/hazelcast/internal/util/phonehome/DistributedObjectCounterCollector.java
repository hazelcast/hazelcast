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

import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.ringbuffer.impl.RingbufferService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

class DistributedObjectCounterCollector implements MetricsCollector {

    private static final Map<String, String> mapMetric;

    static {
        mapMetric = new HashMap<>();
        mapMetric.put(MapService.SERVICE_NAME, "mpct");
        mapMetric.put(SetService.SERVICE_NAME, "sect");
        mapMetric.put(QueueService.SERVICE_NAME, "quct");
        mapMetric.put(MultiMapService.SERVICE_NAME, "mmct");
        mapMetric.put(ListService.SERVICE_NAME, "lict");
        mapMetric.put(RingbufferService.SERVICE_NAME, "rbct");
    }

    @Override
    public Map<String, String> computeMetrics(Node hazelcastNode) {

        Collection<DistributedObject> distributedObjects = hazelcastNode.hazelcastInstance.getDistributedObjects();

        Map<String, Long> countDistributedObjects = new HashMap<>(distributedObjects.stream()
                .filter(distributedObject -> mapMetric.containsKey(distributedObject.getServiceName()))
                .collect(groupingBy(DistributedObject::getServiceName, Collectors.counting())));

        Map<String, String> countInfo = new HashMap<>();

        mapMetric.forEach((k, v) -> countInfo.put(v, String.valueOf(countDistributedObjects.getOrDefault(k, 0L))));

        return countInfo;
    }

}

