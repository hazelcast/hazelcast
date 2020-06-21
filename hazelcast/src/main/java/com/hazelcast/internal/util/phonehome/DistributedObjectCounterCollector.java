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
        final int[] count = {0, 0, 0, 0, 0, 0};

        Collection<DistributedObject> distributedObjects = hazelcastNode.hazelcastInstance.getDistributedObjects();

        distributedObjects.stream().forEach(distributedObject -> {
            switch (distributedObject.getServiceName()) {
                case MapService.SERVICE_NAME:
                    count[0]++;
                    break;
                case SetService.SERVICE_NAME:
                    count[1]++;
                    break;
                case QueueService.SERVICE_NAME:
                    count[2]++;
                    break;
                case MultiMapService.SERVICE_NAME:
                    count[3]++;
                    break;
                case ListService.SERVICE_NAME:
                    count[4]++;
                    break;
                case RingbufferService.SERVICE_NAME:
                    count[5]++;
                    break;
            }
        });

        Map<String, String> countInfo = new HashMap<>();

        countInfo.put("mpct", String.valueOf(count[0]));
        countInfo.put("sect", String.valueOf(count[1]));
        countInfo.put("quct", String.valueOf(count[2]));
        countInfo.put("mmct", String.valueOf(count[3]));
        countInfo.put("lict", String.valueOf(count[4]));
        countInfo.put("rbct", String.valueOf(count[5]));

        return countInfo;
    }

}

