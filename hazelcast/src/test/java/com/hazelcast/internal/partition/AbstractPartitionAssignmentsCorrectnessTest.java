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
import com.hazelcast.nio.Address;
import com.hazelcast.test.AssertTask;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractPartitionAssignmentsCorrectnessTest extends PartitionCorrectnessTestSupport {

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionAssignments_whenNodesStartedTerminated() throws InterruptedException {
        Config config = getConfig(backupCount, false, false);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        warmUpPartitions(hz);

        int size = 1;
        while (size < (nodeCount + 1)) {
            startNodes(config, backupCount + 1);
            size += (backupCount + 1);

            terminateNodes(backupCount);
            size -= backupCount;

            assertPartitionAssignments();
        }
    }

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionAssignments_whenNodesStartedTerminated_withRestart() throws InterruptedException {
        Config config = getConfig(backupCount, false, false);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        warmUpPartitions(hz);

        Collection<Address> addresses = Collections.emptySet();

        int size = 1;
        while (size < (nodeCount + 1)) {
            int startCount = (backupCount + 1) - addresses.size();
            startNodes(config, addresses);
            startNodes(config, startCount);
            size += (backupCount + 1);

            assertPartitionAssignments();

            addresses = terminateNodes(backupCount);
            size -= backupCount;
        }
    }

    private void assertPartitionAssignments() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
                final int actualBackupCount = Math.min(backupCount, instances.size() - 1);

                for (HazelcastInstance hz : instances) {
                    Node node = getNode(hz);
                    InternalPartitionService partitionService = node.getPartitionService();
                    InternalPartition[] partitions = partitionService.getInternalPartitions();

                    for (InternalPartition partition : partitions) {
                        for (int i = 0; i <= actualBackupCount; i++) {
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
