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
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.crdt.pncounter.PNCounterService;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.impl.proxyservice.InternalProxyService;
import com.hazelcast.topic.impl.TopicService;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_CACHES;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_CACHES_ALL_TIME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_CARDINALITY_ESTIMATORS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_CARDINALITY_ESTIMATORS_ALL_TIME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_FLAKE_ID_GENERATORS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_FLAKE_ID_GENERATORS_ALL_TIME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_LISTS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_LISTS_ALL_TIME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_MAPS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_MAPS_ALL_TIME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_MULTIMAPS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_MULTIMAPS_ALL_TIME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_PN_COUNTERS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_PN_COUNTERS_ALL_TIME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_QUEUES;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_QUEUES_ALL_TIME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_REPLICATED_MAPS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_REPLICATED_MAPS_ALL_TIME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_RING_BUFFERS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_RING_BUFFERS_ALL_TIME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_SETS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_SETS_ALL_TIME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_TOPICS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.COUNT_OF_TOPICS_ALL_TIME;
import static com.hazelcast.spi.impl.proxyservice.impl.ProxyRegistry.INTERNAL_OBJECTS_PREFIXES;
import static java.util.stream.Collectors.groupingBy;

@SuppressWarnings("checkstyle:magicnumber")
class DistributedObjectCounterCollector implements MetricsCollector {

    private static final Map<String, PhoneHomeMetrics[]> SERVICE_NAME_TO_METRIC_NAME;

    static {
        SERVICE_NAME_TO_METRIC_NAME = MapUtil.createHashMap(12);
        SERVICE_NAME_TO_METRIC_NAME.put(MapService.SERVICE_NAME,
                new PhoneHomeMetrics[]{COUNT_OF_MAPS, COUNT_OF_MAPS_ALL_TIME});
        SERVICE_NAME_TO_METRIC_NAME.put(SetService.SERVICE_NAME,
                new PhoneHomeMetrics[]{COUNT_OF_SETS, COUNT_OF_SETS_ALL_TIME});
        SERVICE_NAME_TO_METRIC_NAME.put(QueueService.SERVICE_NAME,
                new PhoneHomeMetrics[]{COUNT_OF_QUEUES, COUNT_OF_QUEUES_ALL_TIME});
        SERVICE_NAME_TO_METRIC_NAME.put(MultiMapService.SERVICE_NAME,
                new PhoneHomeMetrics[]{COUNT_OF_MULTIMAPS, COUNT_OF_MULTIMAPS_ALL_TIME});
        SERVICE_NAME_TO_METRIC_NAME.put(ListService.SERVICE_NAME,
                new PhoneHomeMetrics[]{COUNT_OF_LISTS, COUNT_OF_LISTS_ALL_TIME});
        SERVICE_NAME_TO_METRIC_NAME.put(RingbufferService.SERVICE_NAME,
                new PhoneHomeMetrics[]{COUNT_OF_RING_BUFFERS, COUNT_OF_RING_BUFFERS_ALL_TIME});
        SERVICE_NAME_TO_METRIC_NAME.put(CacheService.SERVICE_NAME,
                new PhoneHomeMetrics[]{COUNT_OF_CACHES, COUNT_OF_CACHES_ALL_TIME});
        SERVICE_NAME_TO_METRIC_NAME.put(TopicService.SERVICE_NAME,
                new PhoneHomeMetrics[]{COUNT_OF_TOPICS, COUNT_OF_TOPICS_ALL_TIME});
        SERVICE_NAME_TO_METRIC_NAME.put(ReplicatedMapService.SERVICE_NAME,
                new PhoneHomeMetrics[]{COUNT_OF_REPLICATED_MAPS, COUNT_OF_REPLICATED_MAPS_ALL_TIME});
        SERVICE_NAME_TO_METRIC_NAME.put(CardinalityEstimatorService.SERVICE_NAME,
                new PhoneHomeMetrics[]{COUNT_OF_CARDINALITY_ESTIMATORS, COUNT_OF_CARDINALITY_ESTIMATORS_ALL_TIME});
        SERVICE_NAME_TO_METRIC_NAME.put(PNCounterService.SERVICE_NAME,
                new PhoneHomeMetrics[]{COUNT_OF_PN_COUNTERS, COUNT_OF_PN_COUNTERS_ALL_TIME});
        SERVICE_NAME_TO_METRIC_NAME.put(FlakeIdGeneratorService.SERVICE_NAME,
                new PhoneHomeMetrics[]{COUNT_OF_FLAKE_ID_GENERATORS, COUNT_OF_FLAKE_ID_GENERATORS_ALL_TIME});
    }

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        InternalProxyService proxyService = node.nodeEngine.getProxyService();
        Map<String, Long> objectsPerService = proxyService.getAllDistributedObjects().stream()
                .filter(obj -> INTERNAL_OBJECTS_PREFIXES.stream().noneMatch(prefix -> obj.getName().startsWith(prefix)))
                .filter(obj -> SERVICE_NAME_TO_METRIC_NAME.containsKey(obj.getServiceName()))
                .collect(groupingBy(DistributedObject::getServiceName, Collectors.counting()));

        SERVICE_NAME_TO_METRIC_NAME.forEach((serviceName, metricNames) -> {
            metricsConsumer.accept(
                    metricNames[0],
                    String.valueOf(objectsPerService.getOrDefault(serviceName, 0L)));

            metricsConsumer.accept(
                    metricNames[1],
                    String.valueOf(proxyService.getCreatedCount(serviceName)));
        });

    }
}

