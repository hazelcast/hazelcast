/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.proxyservice.InternalProxyService;
import com.hazelcast.topic.impl.TopicService;

import java.util.Map;
import java.util.Map.Entry;
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

class DistributedObjectMetricsProvider implements MetricsProvider {

    private static final Map<String, Metric[]> SERVICE_NAME_TO_METRIC_NAME = Map.ofEntries(
            entry(MapService.SERVICE_NAME, COUNT_OF_MAPS, COUNT_OF_MAPS_ALL_TIME),
            entry(SetService.SERVICE_NAME, COUNT_OF_SETS, COUNT_OF_SETS_ALL_TIME),
            entry(QueueService.SERVICE_NAME, COUNT_OF_QUEUES, COUNT_OF_QUEUES_ALL_TIME),
            entry(MultiMapService.SERVICE_NAME, COUNT_OF_MULTIMAPS, COUNT_OF_MULTIMAPS_ALL_TIME),
            entry(ListService.SERVICE_NAME, COUNT_OF_LISTS, COUNT_OF_LISTS_ALL_TIME),
            entry(RingbufferService.SERVICE_NAME, COUNT_OF_RING_BUFFERS, COUNT_OF_RING_BUFFERS_ALL_TIME),
            entry(CacheService.SERVICE_NAME, COUNT_OF_CACHES, COUNT_OF_CACHES_ALL_TIME),
            entry(TopicService.SERVICE_NAME, COUNT_OF_TOPICS, COUNT_OF_TOPICS_ALL_TIME),
            entry(ReplicatedMapService.SERVICE_NAME, COUNT_OF_REPLICATED_MAPS, COUNT_OF_REPLICATED_MAPS_ALL_TIME),
            entry(CardinalityEstimatorService.SERVICE_NAME, COUNT_OF_CARDINALITY_ESTIMATORS,
                    COUNT_OF_CARDINALITY_ESTIMATORS_ALL_TIME),
            entry(PNCounterService.SERVICE_NAME, COUNT_OF_PN_COUNTERS, COUNT_OF_PN_COUNTERS_ALL_TIME),
            entry(FlakeIdGeneratorService.SERVICE_NAME, COUNT_OF_FLAKE_ID_GENERATORS, COUNT_OF_FLAKE_ID_GENERATORS_ALL_TIME)
    );

    private static Entry<String, Metric[]> entry(String serviceName, Metric count, Metric countOfAllTime) {
        return Map.entry(serviceName, new Metric[]{count, countOfAllTime});
    }

    @Override
    public void provideMetrics(Node node, MetricsCollectionContext context) {
        InternalProxyService proxyService = node.nodeEngine.getProxyService();
        Map<String, Long> objectsPerService = proxyService.getAllDistributedObjects().stream()
                .filter(obj -> INTERNAL_OBJECTS_PREFIXES.stream().noneMatch(prefix -> obj.getName().startsWith(prefix)))
                .filter(obj -> SERVICE_NAME_TO_METRIC_NAME.containsKey(obj.getServiceName()))
                .collect(groupingBy(DistributedObject::getServiceName, Collectors.counting()));

        SERVICE_NAME_TO_METRIC_NAME.forEach((serviceName, metrics) -> {
            context.collect(metrics[0], objectsPerService.getOrDefault(serviceName, 0L));
            context.collect(metrics[1], proxyService.getCreatedCount(serviceName));
        });
    }
}

