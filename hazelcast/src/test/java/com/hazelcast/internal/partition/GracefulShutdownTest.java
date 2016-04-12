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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.instance.TestUtil.terminateInstance;

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

    @Test
    public void shutdownMultipleMembers_whilePartitionsMigrating() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "12");
        config.setProperty(GroupProperty.PARTITION_MIGRATION_INTERVAL.getName(), "1");

        HazelcastInstance master = factory.newHazelcastInstance(config);
        warmUpPartitions(master);

        HazelcastInstance[] slaves = factory.newInstances(config, 5);

        final List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>(slaves.length + 1);
        instances.add(master);
        instances.addAll(Arrays.asList(slaves));
        Collections.shuffle(instances);

        final int count = instances.size() / 2;
        final CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            final int index = i;
            new Thread() {
                public void run() {
                    HazelcastInstance instance = instances.get(index);
                    instance.shutdown();
                    latch.countDown();
                }
            }.start();
        }

        assertOpenEventually(latch);
        assertPartitionAssignments();
    }

    @Test
    public void shutdownAndTerminateSlaveMembers_concurrently() {
        HazelcastInstance[] instances = factory.newInstances(new Config(), 5);
        int shutdownIndex = RandomPicker.getInt(1, instances.length);
        int terminateIndex;
        do {
            terminateIndex = RandomPicker.getInt(1, instances.length);
        } while (terminateIndex == shutdownIndex);

        shutdownAndTerminateMembers_concurrently(instances, shutdownIndex, terminateIndex);
    }

    @Test
    public void shutdownMasterAndTerminateSlaveMember_concurrently() {
        HazelcastInstance[] instances = factory.newInstances(new Config(), 5);
        int shutdownIndex = 0;
        int terminateIndex = RandomPicker.getInt(1, instances.length);

        shutdownAndTerminateMembers_concurrently(instances, shutdownIndex, terminateIndex);
    }

    @Test
    public void shutdownSlaveAndTerminateMasterMember_concurrently() {
        HazelcastInstance[] instances = factory.newInstances(new Config(), 5);
        int shutdownIndex = RandomPicker.getInt(1, instances.length);
        int terminateIndex = 0;

        shutdownAndTerminateMembers_concurrently(instances, shutdownIndex, terminateIndex);
    }

    private void shutdownAndTerminateMembers_concurrently(HazelcastInstance[] instances,
            int shutdownIndex, int terminateIndex) {

        warmUpPartitions(instances);

        final HazelcastInstance shuttingDownInstance = instances[shutdownIndex];
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                shuttingDownInstance.shutdown();
                latch.countDown();
            }
        }.start();

        // spin until node starts to shut down
        Node shuttingDownNode = getNode(shuttingDownInstance);
        while (shuttingDownNode.isRunning());

        terminateInstance(instances[terminateIndex]);

        assertOpenEventually(latch);
        assertPartitionAssignments();
    }

    private void assertPartitionAssignments() {
        PartitionAssignmentsCorrectnessTest.assertPartitionAssignments(factory);
    }
}
