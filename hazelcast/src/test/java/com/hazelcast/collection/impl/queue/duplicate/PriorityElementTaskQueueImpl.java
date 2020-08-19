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

package com.hazelcast.collection.impl.queue.duplicate;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.impl.queue.model.PriorityElement;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriorityElementTaskQueueImpl extends HazelcastTestSupport implements PriorityElementTaskQueue {

    public static final String PRIORITY_ELEMENT_QUEUE_NAME = "PRIORITY_ELEMENT_QUEUE";
    public static final String PRIORITY_ELEMENT_MAP_NAME = "PRIORITY_ELEMENT_MAP";

    private static final Logger LOG = LoggerFactory.getLogger(PriorityElementTaskQueueImpl.class);

    private final IQueue<PriorityElement> queue;

    private final IMap<PriorityElement, PriorityElement> map;

    public PriorityElementTaskQueueImpl() {

        Config config = new Config();
        config.getQueueConfig(PRIORITY_ELEMENT_QUEUE_NAME)
              .setPriorityComparatorClassName("com.hazelcast.collection.impl.queue.model.PriorityElementComparator");
        HazelcastInstance hz = createHazelcastInstance(config);
        queue = hz.getQueue(PRIORITY_ELEMENT_QUEUE_NAME);
        map = hz.getMap(PRIORITY_ELEMENT_MAP_NAME);

    }

    @Override
    public boolean enqueue(final PriorityElement fahrplanTask) {
        try {
            PriorityElement previousValue = map.get(fahrplanTask);

            if (previousValue != null) {
                return false;
            }

            boolean added = queue.offer(fahrplanTask);
            if (added) {
                map.put(fahrplanTask, fahrplanTask);
            }
            return added;
        } catch (Exception e) {
            LOG.warn("Unable to write to priorityQueue: " + e);
            return false;
        }

    }

    @Override
    public PriorityElement dequeue() {
        try {
            final PriorityElement element = queue.poll();
            if (element != null) {
                map.remove(element);
            }
            return element;
        } catch (Exception e) {
            LOG.warn("Unable to read from priorityQueue: " + e);
            return null;
        }
    }

    @Override
    public void clear() {
        try {
            queue.clear();
            map.clear();
        } catch (Exception e) {
            LOG.warn("Unable to clear priorityQueue", e);
        }
    }
}
