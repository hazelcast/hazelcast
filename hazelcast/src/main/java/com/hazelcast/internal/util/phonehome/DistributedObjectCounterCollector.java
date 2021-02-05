/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

class DistributedObjectCounterCollector implements MetricsCollector {

    private static final Map<String, PhoneHomeMetrics> SERVICE_NAME_TO_METRIC_NAME;

    static {
        SERVICE_NAME_TO_METRIC_NAME = new HashMap<>();
        SERVICE_NAME_TO_METRIC_NAME.put(MapService.SERVICE_NAME, PhoneHomeMetrics.COUNT_OF_MAPS);
        SERVICE_NAME_TO_METRIC_NAME.put(SetService.SERVICE_NAME, PhoneHomeMetrics.COUNT_OF_SETS);
        SERVICE_NAME_TO_METRIC_NAME.put(QueueService.SERVICE_NAME, PhoneHomeMetrics.COUNT_OF_QUEUES);
        SERVICE_NAME_TO_METRIC_NAME.put(MultiMapService.SERVICE_NAME, PhoneHomeMetrics.COUNT_OF_MULTIMAPS);
        SERVICE_NAME_TO_METRIC_NAME.put(ListService.SERVICE_NAME, PhoneHomeMetrics.COUNT_OF_LISTS);
        SERVICE_NAME_TO_METRIC_NAME.put(RingbufferService.SERVICE_NAME, PhoneHomeMetrics.COUNT_OF_RING_BUFFERS);
        SERVICE_NAME_TO_METRIC_NAME.put(CacheService.SERVICE_NAME, PhoneHomeMetrics.COUNT_OF_CACHES);
        SERVICE_NAME_TO_METRIC_NAME.put(TopicService.SERVICE_NAME, PhoneHomeMetrics.COUNT_OF_TOPICS);
        SERVICE_NAME_TO_METRIC_NAME.put(ReplicatedMapService.SERVICE_NAME, PhoneHomeMetrics.COUNT_OF_REPLICATED_MAPS);
        SERVICE_NAME_TO_METRIC_NAME.put(CardinalityEstimatorService.SERVICE_NAME,
                PhoneHomeMetrics.COUNT_OF_CARDINALITY_ESTIMATORS);
        SERVICE_NAME_TO_METRIC_NAME.put(PNCounterService.SERVICE_NAME, PhoneHomeMetrics.COUNT_OF_PN_COUNTERS);
        SERVICE_NAME_TO_METRIC_NAME.put(FlakeIdGeneratorService.SERVICE_NAME, PhoneHomeMetrics.COUNT_OF_FLAKE_ID_GENERATORS);
    }

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        Collection<DistributedObject> objects = node.hazelcastInstance.getDistributedObjects();
        Map<String, Long> objectsPerService =
                objects.stream()
                       .filter(obj -> SERVICE_NAME_TO_METRIC_NAME.containsKey(obj.getServiceName()))
                       .collect(groupingBy(DistributedObject::getServiceName, Collectors.counting()));

        SERVICE_NAME_TO_METRIC_NAME.forEach((serviceName, metricName) ->
                metricsConsumer.accept(
                        metricName,
                        String.valueOf(objectsPerService.getOrDefault(serviceName, 0L))));
    }
}

