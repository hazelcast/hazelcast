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
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.crdt.pncounter.PNCounterService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.topic.impl.TopicService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

class DistributedObjectCounterCollector implements MetricsCollector {

    private static final Map<String, String> SERVICE_NAME_TO_METRIC_NAME;

    static {
        SERVICE_NAME_TO_METRIC_NAME = new HashMap<>();
        SERVICE_NAME_TO_METRIC_NAME.put(MapService.SERVICE_NAME, "mpct");
        SERVICE_NAME_TO_METRIC_NAME.put(SetService.SERVICE_NAME, "sect");
        SERVICE_NAME_TO_METRIC_NAME.put(QueueService.SERVICE_NAME, "quct");
        SERVICE_NAME_TO_METRIC_NAME.put(MultiMapService.SERVICE_NAME, "mmct");
        SERVICE_NAME_TO_METRIC_NAME.put(ListService.SERVICE_NAME, "lict");
        SERVICE_NAME_TO_METRIC_NAME.put(RingbufferService.SERVICE_NAME, "rbct");
        SERVICE_NAME_TO_METRIC_NAME.put(CacheService.SERVICE_NAME, "cact");
        SERVICE_NAME_TO_METRIC_NAME.put(TopicService.SERVICE_NAME, "tpct");
        SERVICE_NAME_TO_METRIC_NAME.put(ReplicatedMapService.SERVICE_NAME, "rpct");
        SERVICE_NAME_TO_METRIC_NAME.put(CardinalityEstimatorService.SERVICE_NAME, "cect");
        SERVICE_NAME_TO_METRIC_NAME.put(PNCounterService.SERVICE_NAME, "pncct");
        SERVICE_NAME_TO_METRIC_NAME.put(FlakeIdGeneratorService.SERVICE_NAME, "figct");
    }

    @Override
    public Map<String, String> computeMetrics(Node hazelcastNode) {

        Collection<DistributedObject> distributedObjects = hazelcastNode.hazelcastInstance.getDistributedObjects();

        Map<String, Long> countDistributedObjects = new HashMap<>(distributedObjects.stream()
                .filter(distributedObject -> SERVICE_NAME_TO_METRIC_NAME.containsKey(distributedObject.getServiceName()))
                .collect(groupingBy(DistributedObject::getServiceName, Collectors.counting())));

        Map<String, String> countInfo = new HashMap<>();

        SERVICE_NAME_TO_METRIC_NAME.forEach((serviceName, metricName) -> countInfo
                .put(metricName, String.valueOf(countDistributedObjects.getOrDefault(serviceName, 0L))));

        return countInfo;
    }

}

