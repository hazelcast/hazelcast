/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionListenerTest extends HazelcastTestSupport {

    @Test
    public void test_initialAssignment() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance();

        HazelcastInstance hz = factory.newHazelcastInstance();
        InternalPartitionServiceImpl partitionService = getNode(hz).partitionService;

        final int partitionCount = partitionService.getPartitionCount();

        final AtomicInteger count = addEventCountingPartitionListener(partitionService);

        warmUpPartitions(hz);

        final int expectedEventCount = 2 * partitionCount;
        assertPartitionEventsEventually(expectedEventCount, count);
    }

    private void assertPartitionEventsEventually(final int expectedEventCount, final AtomicInteger count) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expectedEventCount, count.get());
            }
        });
    }

    @Test
    public void test_whenMemberAdded() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz = factory.newHazelcastInstance();
        InternalPartitionServiceImpl partitionService = getNode(hz).partitionService;

        final int partitionCount = partitionService.getPartitionCount();
        warmUpPartitionsAndDrainEvents(hz, partitionCount);

        final AtomicInteger count = addEventCountingPartitionListener(partitionService);

        factory.newHazelcastInstance();
        assertClusterSizeEventually(2, hz);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int currentCount = count.get();
                assertTrue("Expecting events equal or greater than partition-count! Count: " + currentCount,
                        currentCount >= partitionCount);
            }
        });
    }

    @Test
    public void test_whenMemberRemoved() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        InternalPartitionServiceImpl partitionService = getNode(hz2).partitionService;
        final int partitionCount = partitionService.getPartitionCount();

        warmUpPartitionsAndDrainEvents(hz2, partitionCount * 2);

        final AtomicInteger count = addEventCountingPartitionListener(partitionService);

        hz1.getLifecycleService().terminate();
        assertClusterSizeEventually(1, hz2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int currentCount = count.get();
                assertTrue("Expecting events equal or greater than partition-count! Count: " + currentCount,
                        currentCount >= partitionCount);
            }
        });
    }

    private AtomicInteger addEventCountingPartitionListener(InternalPartitionServiceImpl partitionService) {
        final AtomicInteger counter = new AtomicInteger();
        partitionService.addPartitionListener(new EventCountingPartitionListener(counter));
        return counter;
    }

    private void warmUpPartitionsAndDrainEvents(HazelcastInstance hz, int count) {
        final CountDownLatch latch = new CountDownLatch(count);
        InternalPartitionServiceImpl partitionService = getNode(hz).partitionService;
        partitionService.addPartitionListener(new PartitionListener() {
            @Override
            public void replicaChanged(PartitionReplicaChangeEvent event) {
                latch.countDown();
            }
        });
        warmUpPartitions(hz);
        assertOpenEventually(latch);
    }

    private static class EventCountingPartitionListener implements PartitionListener {
        private final AtomicInteger count;

        public EventCountingPartitionListener(AtomicInteger count) {
            this.count = count;
        }

        @Override
        public void replicaChanged(PartitionReplicaChangeEvent event) {
            count.incrementAndGet();
        }
    }
}
