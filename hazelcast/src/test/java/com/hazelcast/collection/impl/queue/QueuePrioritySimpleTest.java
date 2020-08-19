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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.impl.queue.model.PriorityElement;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueuePrioritySimpleTest extends HazelcastTestSupport {

    private IQueue<PriorityElement> queue;

    @Before
    public void before() {
        String queueName = randomString();
        Config config = new Config();
        config.getQueueConfig(queueName)
              .setPriorityComparatorClassName("com.hazelcast.collection.impl.queue.model.PriorityElementComparator");
        HazelcastInstance hz = createHazelcastInstance(config);
        queue = hz.getQueue(queueName);
    }

    @Test
    public void testPriorityQueue_whenHighestOfferedSecond_thenTakeHighest() throws Exception {

        PriorityElement elementLow = new PriorityElement(false, 1);
        PriorityElement elementHigh = new PriorityElement(true, 1);

        assertTrue(queue.offer(elementLow));
        assertTrue(queue.offer(elementHigh));
        assertEquals(2, queue.size());
        assertEquals(true, queue.poll().isHighPriority());
        assertEquals(false, queue.poll().isHighPriority());
        assertEquals(0, queue.size());
    }

    @Test
    public void testPriorityQueue_whenHighestOfferedFirst_thenTakeHighest() throws Exception {

        PriorityElement elementLow = new PriorityElement(false, 1);
        PriorityElement elementHigh = new PriorityElement(true, 1);

        assertTrue(queue.offer(elementHigh));
        assertTrue(queue.offer(elementLow));
        assertEquals(2, queue.size());
        assertEquals(true, queue.poll().isHighPriority());
        assertEquals(false, queue.poll().isHighPriority());
        assertEquals(0, queue.size());
    }

    @Test
    public void testPriorityQueue_whenTwoHighest_thenTakeFirstVersion() throws Exception {

        PriorityElement elementHigh1 = new PriorityElement(true, 1);
        PriorityElement elementHigh2 = new PriorityElement(true, 2);

        assertTrue(queue.offer(elementHigh1));
        assertTrue(queue.offer(elementHigh2));
        assertEquals(2, queue.size());
        assertEquals(Integer.valueOf(1), queue.poll().getVersion());
        assertEquals(Integer.valueOf(2), queue.poll().getVersion());
        assertEquals(0, queue.size());
    }

    @Test
    public void testPriorityQueue_whenTwoHighest_thenTakeFirstVersionAgain() throws Exception {

        PriorityElement elementHigh1 = new PriorityElement(true, 1);
        PriorityElement elementHigh2 = new PriorityElement(true, 2);

        assertTrue(queue.offer(elementHigh2));
        assertTrue(queue.offer(elementHigh1));
        assertEquals(2, queue.size());
        assertEquals(Integer.valueOf(1), queue.poll().getVersion());
        assertEquals(Integer.valueOf(2), queue.poll().getVersion());
        assertEquals(0, queue.size());
    }
}
