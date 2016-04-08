/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.properties.GroupProperty;
import com.hazelcast.nio.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GracefulShutdownTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory();
    }

    @Test(timeout = 60000)
    public void shutdownSingleMember_withoutPartitionInitialization() {
        HazelcastInstance hz = factory.newHazelcastInstance();
        hz.shutdown();
    }

    @Test(timeout = 60000)
    public void shutdownSingleMember_withPartitionInitialization() {
        HazelcastInstance hz = factory.newHazelcastInstance();
        warmUpPartitions(hz);
        hz.shutdown();
    }

    @Test(timeout = 60000)
    public void shutdownSingleLiteMember() {
        HazelcastInstance hz = factory.newHazelcastInstance(new Config().setLiteMember(true));
        hz.shutdown();
    }

    @Test(timeout = 60000)
    public void shutdownSlaveMember_withoutPartitionInitialization() {
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();

        hz2.shutdown();
    }

    @Test(timeout = 60000)
    public void shutdownSlaveMember_withPartitionInitialization() {
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();

        warmUpPartitions(hz1, hz2, hz3);
        hz2.shutdown();
        assertPartitionAssignments();
    }

    @Test(timeout = 60000)
    public void shutdownSlaveMember_whilePartitionsMigrating() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "12");
        config.setProperty(GroupProperty.PARTITION_MIGRATION_INTERVAL.getName(), "1");

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        warmUpPartitions(hz1);

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        hz2.shutdown();

        assertPartitionAssignments();
    }

    @Test(timeout = 60000)
    public void shutdownSlaveLiteMember() {
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz3 = factory.newHazelcastInstance();

        warmUpPartitions(hz1, hz2, hz3);
        hz2.shutdown();

        assertPartitionAssignments();
    }

    @Test(timeout = 60000)
    public void shutdownMasterMember_withoutPartitionInitialization() {
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();

        hz1.shutdown();
    }

    @Test(timeout = 60000)
    public void shutdownMasterMember_withPartitionInitialization() {
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();

        warmUpPartitions(hz1, hz2, hz3);
        hz1.shutdown();

        assertPartitionAssignments();
    }

    @Test(timeout = 60000)
    public void shutdownMasterMember_whilePartitionsMigrating() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "12");
        config.setProperty(GroupProperty.PARTITION_MIGRATION_INTERVAL.getName(), "1");

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        warmUpPartitions(hz1);

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        hz1.shutdown();

        assertPartitionAssignments();
    }

    @Test(timeout = 60000)
    public void shutdownMasterLiteMember() {
        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();

        warmUpPartitions(hz1, hz2, hz3);
        hz1.shutdown();

        assertPartitionAssignments();
    }

    @Test(timeout = 60000)
    public void shutdownMultipleSlaveMembers_withoutPartitionInitialization() {
        shutdownMultipleMembers(false, false);
    }

    @Test(timeout = 60000)
    public void shutdownMultipleMembers_withoutPartitionInitialization() {
       shutdownMultipleMembers(true, false);
    }

    @Test(timeout = 60000)
    public void shutdownMultipleSlaveMembers_withPartitionInitialization() {
        shutdownMultipleMembers(false, true);
    }

    @Test(timeout = 60000)
    public void shutdownMultipleMembers_withPartitionInitialization() {
        shutdownMultipleMembers(true, true);
    }

    private void shutdownMultipleMembers(boolean includeMaster, boolean initializePartitions) {
        final HazelcastInstance[] instances = factory.newInstances(new Config(), 6);

        if (initializePartitions) {
            warmUpPartitions(instances);
        }

        final CountDownLatch latch = new CountDownLatch(instances.length / 2);
        int startIndex = includeMaster ? 0 : 1;
        for (int i = startIndex; i < instances.length; i += 2) {
            final int index = i;
            new Thread() {
                public void run() {
                    instances[index].shutdown();
                    latch.countDown();
                }
            }.start();
        }

        assertOpenEventually(latch);

        if (initializePartitions) {
            assertPartitionAssignments();
        }
    }

    // TODO: add more tests
    // - shutdown multiple members while partitions migrating
    // - terminate another member while a member is shutting down
    // -

    private void assertPartitionAssignments() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
                final int replicaCount = Math.min(instances.size(), InternalPartition.MAX_REPLICA_COUNT);

                for (HazelcastInstance hz : instances) {
                    Node node = getNode(hz);
                    InternalPartitionService partitionService = node.getPartitionService();
                    InternalPartition[] partitions = partitionService.getInternalPartitions();

                    for (InternalPartition partition : partitions) {
                        for (int i = 0; i < replicaCount; i++) {
                            Address replicaAddress = partition.getReplicaAddress(i);
                            assertNotNull("Replica " + i + " is not found in " + partition, replicaAddress);
                            assertTrue("Not member: " + replicaAddress, node.getClusterService().getMember(replicaAddress) != null);
                        }
                    }
                }
            }
        });
    }
}
