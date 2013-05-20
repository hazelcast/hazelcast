/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.StaticNodeFactory;
import com.hazelcast.test.RandomBlockJUnit4ClassRunner;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @mdogan 5/20/13
 */

@RunWith(RandomBlockJUnit4ClassRunner.class)
public class PartitionDistributionTest {

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTwoNodesDefault() throws InterruptedException {
        testPartitionDistribution(271, 2);
    }

    @Test
    public void testTwoNodes1111Partitions() throws InterruptedException {
        testPartitionDistribution(1111, 2);
    }

    @Test
    public void testFiveNodesDefault() throws InterruptedException {
        testPartitionDistribution(271, 5);
    }

    @Test
    public void testFiveNodes1111Partitions() throws InterruptedException {
        testPartitionDistribution(1111, 5);
    }

    @Test
    public void testTenNodesDefault() throws InterruptedException {
        testPartitionDistribution(271, 10);
    }

    @Test
    public void testTenNodes1111Partitions() throws InterruptedException {
        testPartitionDistribution(1111, 10);
    }

    private void testPartitionDistribution(final int partitionCount, final int nodeCount) throws InterruptedException {
        final Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, String.valueOf(partitionCount));

        StaticNodeFactory factory = new StaticNodeFactory(nodeCount);
        final BlockingQueue<Integer> counts = new ArrayBlockingQueue<Integer>(nodeCount);
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }
        for (int j = 0; j < nodeCount; j++) {
            final int instanceIndex = j;
            new Thread(new Runnable() {
                public void run() {
                    final HazelcastInstance h = instances[instanceIndex];
                    h.getMap("test").size(); // to start partition assignment
                    counts.offer(getLocalPartitionsCount(h));
                }
            }).start();
        }

        final int average = (partitionCount / nodeCount);
        int total = 0;
        for (int i = 0; i < nodeCount; i++) {
            final int c = counts.take();
            assertTrue("Partition count of node[" + i + "] is : " + c
                    + ", total partitions: " + partitionCount + ", nodes: " + nodeCount, c >= average);
            total += c;
        }
        assertEquals(partitionCount, total);
    }

    private int getLocalPartitionsCount(HazelcastInstance h) {
        final Member localMember = h.getCluster().getLocalMember();
        Set<Partition> partitions = h.getPartitionService().getPartitions();
        int count = 0;
        for (Partition partition : partitions) {
            if (localMember.equals(partition.getOwner())) {
                count++;
            }
        }
        return count;
    }
}
