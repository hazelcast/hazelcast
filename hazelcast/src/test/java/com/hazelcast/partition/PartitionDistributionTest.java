/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class PartitionDistributionTest extends HazelcastTestSupport {

    private Config hostAwareConfig = new Config();

    @BeforeClass
    @AfterClass
    public static void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUp() {
        PartitionGroupConfig partitionGroupConfig = hostAwareConfig.getPartitionGroupConfig();
        partitionGroupConfig
                .setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.HOST_AWARE);
    }

    @Test
    public void testTwoNodes_defaultPartitions() throws InterruptedException {
        testPartitionDistribution(271, 2);
    }

    @Test
    public void testTwoNodes_1111Partitions() throws InterruptedException {
        testPartitionDistribution(1111, 2);
    }

    @Test
    public void testTwoNodes_defaultPartitions_HostAware() throws InterruptedException {
        testPartitionDistribution(271, 2, hostAwareConfig);
    }

    @Test
    public void testThreeNodes_defaultPartitions() throws InterruptedException {
        testPartitionDistribution(271, 3);
    }

    @Test(expected = AssertionError.class)
    public void testThreeNodes_defaultPartitions_HostAware() throws InterruptedException {
        testPartitionDistribution(271, 3, hostAwareConfig);
    }

    @Test
    public void testFourNodes_defaultPartitions_HostAware() throws InterruptedException {
        testPartitionDistribution(271, 4, hostAwareConfig);
    }

    @Test
    public void testFiveNodes_defaultPartitions() throws InterruptedException {
        testPartitionDistribution(271, 5);
    }

    @Test
    public void testFiveNodes_1111Partitions() throws InterruptedException {
        testPartitionDistribution(1111, 5);
    }

    @Test(expected = AssertionError.class)
    public void testFiveNodes_defaultPartitions_HostAware() throws InterruptedException {
        testPartitionDistribution(271, 5, hostAwareConfig);
    }

    @Test
    public void testTenNodes_defaultPartitions() throws InterruptedException {
        testPartitionDistribution(271, 10);
    }

    @Test
    public void testTenNodes_1111Partitions() throws InterruptedException {
        testPartitionDistribution(1111, 10);
    }

    @Test
    public void testTenNodes_defaultPartitions_HostAware() throws InterruptedException {
        testPartitionDistribution(271, 10, hostAwareConfig);
    }

    @Test(expected = AssertionError.class)
    public void testFifteenNodes_defaultPartitions_HostAware() throws InterruptedException {
        testPartitionDistribution(271, 15, hostAwareConfig);
    }

    private void testPartitionDistribution(int partitionCount, int nodeCount) throws InterruptedException {
        testPartitionDistribution(partitionCount, nodeCount, new Config());
    }

    private void testPartitionDistribution(int partitionCount, int nodeCount, Config config) throws InterruptedException {
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, String.valueOf(partitionCount));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final BlockingQueue<Integer> counts = new ArrayBlockingQueue<Integer>(nodeCount);
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];

        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }

        ExecutorService ex = Executors.newCachedThreadPool();
        try {
            for (int i = 0; i < nodeCount; i++) {
                final int instanceIndex = i;
                new Thread(new Runnable() {
                    public void run() {
                        HazelcastInstance instance = instances[instanceIndex];
                        warmUpPartitions(instance);
                        counts.offer(getLocalPartitionsCount(instance));
                    }
                }).start();
            }

            ILogger logger = instances[0].getLoggingService().getLogger(getClass());
            String firstFailureMessage = null;

            int average = (partitionCount / nodeCount);
            logger.info(format("Partition count: %d, nodes: %d, average: %d", partitionCount, nodeCount, average));

            int totalPartitions = 0;
            for (int i = 0; i < nodeCount; i++) {
                Integer localPartitionCount = counts.poll(1, TimeUnit.MINUTES);
                assertNotNull(localPartitionCount);

                String msg = format("Node: %d, local partition count: %d", i + 1, localPartitionCount);
                if (firstFailureMessage == null && localPartitionCount < average) {
                    firstFailureMessage = msg;
                }
                logger.info(msg);

                totalPartitions += localPartitionCount;
            }
            assertEqualsStringFormat("Expected sum of local partitions to be %d, but was %d", partitionCount, totalPartitions);

            if (firstFailureMessage != null) {
                fail(format("%s, partition count: %d, nodes: %d, average: %d",
                        firstFailureMessage, partitionCount, nodeCount, average));
            }
        } finally {
            ex.shutdownNow();
        }
    }

    private int getLocalPartitionsCount(HazelcastInstance instance) {
        Member localMember = instance.getCluster().getLocalMember();
        Set<Partition> partitions = instance.getPartitionService().getPartitions();
        int count = 0;
        for (Partition partition : partitions) {
            if (localMember.equals(partition.getOwner())) {
                count++;
            }
        }
        return count;
    }
}
