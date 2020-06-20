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

import static java.util.stream.Collectors.toList;

class DistributedObjectCounterCollector implements MetricsCollector {

    Collection<DistributedObject> maps;
    Collection<DistributedObject> sets;
    Collection<DistributedObject> queues;
    Collection<DistributedObject> multimaps;
    Collection<DistributedObject> lists;
    Collection<DistributedObject> ringbuffers;


    @Override
    public Map<String, String> computeMetrics(Node hazelcastNode) {

        Collection<DistributedObject> distributedObjects = hazelcastNode.hazelcastInstance.getDistributedObjects();

        maps = distributedObjects.stream().filter(distributedObject -> distributedObject.getServiceName().
                equals(MapService.SERVICE_NAME)).collect(toList());
        sets = distributedObjects.stream().filter(distributedObject -> distributedObject.getServiceName().
                equals(SetService.SERVICE_NAME)).collect(toList());
        queues = distributedObjects.stream().filter(distributedObject -> distributedObject.getServiceName().
                equals(QueueService.SERVICE_NAME)).collect(toList());
        multimaps = distributedObjects.stream().filter(distributedObject -> distributedObject.getServiceName().
                equals(MultiMapService.SERVICE_NAME)).collect(toList());
        lists = distributedObjects.stream().filter(distributedObject -> distributedObject.getServiceName().
                equals(ListService.SERVICE_NAME)).collect(toList());
        ringbuffers = distributedObjects.stream().filter(distributedObject -> distributedObject.getServiceName().
                equals(RingbufferService.SERVICE_NAME)).collect(toList());

        Map<String, String> countInfo = new HashMap<>();

        countInfo.put("mpct", String.valueOf(maps.size()));
        countInfo.put("sect", String.valueOf(sets.size()));
        countInfo.put("quct", String.valueOf(queues.size()));
        countInfo.put("mmct", String.valueOf(multimaps.size()));
        countInfo.put("lict", String.valueOf(lists.size()));
        countInfo.put("rbct", String.valueOf(ringbuffers.size()));

        return countInfo;
    }

}

