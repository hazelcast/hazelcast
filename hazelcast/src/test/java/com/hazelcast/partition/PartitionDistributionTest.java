/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.GroupProperty;
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

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class PartitionDistributionTest extends HazelcastTestSupport {

    private Config hostAwareConfig = new Config();

    private Config hostAwareLiteMemberConfig = new Config().setLiteMember(true);

    @BeforeClass
    @AfterClass
    public static void killAllHazelcastInstances() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUp() {
        configureHostAware(hostAwareConfig);
        configureHostAware(hostAwareLiteMemberConfig);
    }

    private void configureHostAware(Config config) {
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        partitionGroupConfig
                .setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.HOST_AWARE);
    }

    @Test
    public void testTwoNodes_defaultPartitions() {
        testPartitionDistribution(271, 2, 0);
    }

    @Test
    public void testTwoNodes_withTwoLiteNodes_defaultPartitions() {
        testPartitionDistribution(271, 2, 2);
    }

    @Test
    public void testTwoNodes_1111Partitions() {
        testPartitionDistribution(1111, 2, 0);
    }

    @Test
    public void testTwoNodes_withTwoLiteNodes_1111Partitions() {
        testPartitionDistribution(1111, 2, 2);
    }

    @Test
    public void testTwoNodes_defaultPartitions_HostAware() {
        testPartitionDistribution(271, 2, 0, hostAwareConfig, hostAwareLiteMemberConfig);
    }

    @Test
    public void testTwoNodes_withTwoLiteNodes_defaultPartitions_HostAware() {
        testPartitionDistribution(271, 2, 2, hostAwareConfig, hostAwareLiteMemberConfig);
    }

    @Test
    public void testThreeNodes_defaultPartitions() {
        testPartitionDistribution(271, 3, 0);
    }

    @Test(expected = AssertionError.class)
    public void testThreeNodes_defaultPartitions_HostAware() {
        testPartitionDistribution(271, 3, 0, hostAwareConfig, hostAwareLiteMemberConfig);
    }

    @Test
    public void testFourNodes_defaultPartitions_HostAware() {
        testPartitionDistribution(271, 4, 0, hostAwareConfig, hostAwareLiteMemberConfig);
    }

    @Test
    public void testFiveNodes_defaultPartitions() {
        testPartitionDistribution(271, 5, 0);
    }

    @Test
    public void testFiveNodes_1111Partitions() {
        testPartitionDistribution(1111, 5, 0);
    }

    @Test(expected = AssertionError.class)
    public void testFiveNodes_defaultPartitions_HostAware() {
        testPartitionDistribution(271, 5, 0, hostAwareConfig, hostAwareLiteMemberConfig);
    }

    @Test
    public void testTenNodes_defaultPartitions() {
        testPartitionDistribution(271, 10, 0);
    }

    @Test
    public void testTenNodes_1111Partitions() {
        testPartitionDistribution(1111, 10, 0);
    }

    @Test
    public void testTenNodes_defaultPartitions_HostAware() {
        testPartitionDistribution(271, 10, 0, hostAwareConfig, hostAwareLiteMemberConfig);
    }

    @Test(expected = AssertionError.class)
    public void testFifteenNodes_defaultPartitions_HostAware() {
        testPartitionDistribution(271, 15, 0, hostAwareConfig, hostAwareLiteMemberConfig);
    }

    private void testPartitionDistribution(int partitionCount, int dataNodeCount, int liteNodeCount) {
        testPartitionDistribution(partitionCount, dataNodeCount, liteNodeCount, new Config(), new Config().setLiteMember(true));
    }

    private void testPartitionDistribution(int partitionCount, int dataNodeCount, int liteNodeCount, Config config,
                                           Config liteConfig) {
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));
        int nodeCount = dataNodeCount + liteNodeCount;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final BlockingQueue<Integer> counts = new ArrayBlockingQueue<Integer>(nodeCount);
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];

        for (int i = 0; i < dataNodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }

        liteConfig.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));
        liteConfig.setLiteMember(true);
        for (int i = dataNodeCount; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(liteConfig);
        }

        Thread[] threads = new Thread[dataNodeCount];
        try {
            for (int i = 0; i < dataNodeCount; i++) {
                final int instanceIndex = i;
                threads[i] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        HazelcastInstance instance = instances[instanceIndex];
                        counts.offer(getLocalPartitionsCount(instance));
                    }
                });
                threads[i].start();
            }

            ILogger logger = instances[0].getLoggingService().getLogger(getClass());
            String firstFailureMessage = null;

            int average = (partitionCount / dataNodeCount);
            logger.info(format("Partition count: %d, nodes: %d, average: %d", partitionCount, dataNodeCount, average));

            int totalPartitions = 0;
            for (int i = 0; i < dataNodeCount; i++) {
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
                fail(format("%s, partition count: %d, nodes: %d, average: %d", firstFailureMessage, partitionCount, dataNodeCount,
                        average));
            }

            for (int i = dataNodeCount; i < nodeCount; i++) {
                assertEquals(0, getLocalPartitionsCount(instances[i]));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw rethrow(e);
        } finally {
            assertJoinable(threads);
        }
    }

    private static int getLocalPartitionsCount(HazelcastInstance instance) {
        warmUpPartitions(instance);

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
