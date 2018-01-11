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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.mocknetwork.MockNodeContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InternalPartitionServiceImplTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private InternalPartitionServiceImpl partitionService;
    private Address thisAddress;
    private int partitionCount;
    private final AtomicBoolean startupDone = new AtomicBoolean(true);

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        NodeContext nodeContext = new MockNodeContext(factory.getRegistry(), factory.nextAddress()) {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                return new DefaultNodeExtension(node) {
                    @Override
                    public boolean isStartCompleted() {
                        return startupDone.get();
                    }
                };
            }
        };

        instance = HazelcastInstanceFactory.newHazelcastInstance(new Config(), randomName(), nodeContext);
        partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        thisAddress = getNode(instance).getThisAddress();
        partitionCount = partitionService.getPartitionCount();
    }

    @Test(expected = HazelcastInstanceNotActiveException.class)
    public void test_getPartitionOwnerOrWait_throwsException_afterNodeShutdown() throws Exception {
        instance.shutdown();
        partitionService.getPartitionOwnerOrWait(0);
    }

    @Test
    public void test_initialAssignment() {
        partitionService.firstArrangement();

        int partitionCount = partitionService.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            assertTrue(partitionService.isPartitionOwner(i));
        }
    }

    @Test
    public void test_initialAssignment_whenStartNotCompleted() {
        startupDone.set(false);

        partitionService.firstArrangement();

        assertFalse(partitionService.getPartitionStateManager().isInitialized());
        assertEquals(0, partitionService.getPartitionStateVersion());
        assertNull(partitionService.getPartitionOwner(0));
    }

    @Test
    public void test_initialAssignment_whenClusterNotActive() {
        instance.getCluster().changeClusterState(ClusterState.FROZEN);

        partitionService.firstArrangement();

        assertFalse(partitionService.getPartitionStateManager().isInitialized());
        assertEquals(0, partitionService.getPartitionStateVersion());
        assertNull(partitionService.getPartitionOwner(0));
    }

    @Test(expected = IllegalStateException.class)
    public void test_getPartitionOwnerOrWait_whenClusterNotActive() {
        instance.getCluster().changeClusterState(ClusterState.FROZEN);

        partitionService.firstArrangement();
        partitionService.getPartitionOwnerOrWait(0);
    }

    @Test
    public void test_setInitialState() {
        Address[][] addresses = new Address[partitionCount][MAX_REPLICA_COUNT];
        for (int i = 0; i < partitionCount; i++) {
            addresses[i][0] = thisAddress;
        }

        partitionService.setInitialState(new PartitionTableView(addresses, partitionCount));
        for (int i = 0; i < partitionCount; i++) {
            assertTrue(partitionService.isPartitionOwner(i));
        }
        assertEquals(partitionCount, partitionService.getPartitionStateVersion());
    }

    @Test(expected = IllegalStateException.class)
    public void test_setInitialState_multipleTimes() {
        Address[][] addresses = new Address[partitionCount][MAX_REPLICA_COUNT];
        for (int i = 0; i < partitionCount; i++) {
            addresses[i][0] = thisAddress;
        }

        partitionService.setInitialState(new PartitionTableView(addresses, 0));
        partitionService.setInitialState(new PartitionTableView(addresses, 0));
    }

    @Test
    public void test_setInitialState_listenerShouldNOTBeCalled() {
        Address[][] addresses = new Address[partitionCount][MAX_REPLICA_COUNT];
        for (int i = 0; i < partitionCount; i++) {
            addresses[i][0] = thisAddress;
        }

        TestPartitionListener listener = new TestPartitionListener();
        partitionService.addPartitionListener(listener);

        partitionService.setInitialState(new PartitionTableView(addresses, 0));
        assertEquals(0, listener.eventCount);
    }

    @Test
    public void test_getMemberPartitions_whenNotInitialized() {
        List<Integer> partitions = partitionService.getMemberPartitions(getAddress(instance));
        assertTrue(partitionService.getPartitionStateManager().isInitialized());
        assertEquals(partitionCount, partitions.size());
    }

    @Test
    public void test_getMemberPartitions_whenInitialized() {
        partitionService.firstArrangement();
        List<Integer> partitions = partitionService.getMemberPartitions(getAddress(instance));
        assertEquals(partitionCount, partitions.size());
    }

    @Test
    public void test_getMemberPartitionsIfAssigned_whenNotInitialized() {
        List<Integer> partitions = partitionService.getMemberPartitionsIfAssigned(getAddress(instance));
        assertThat(partitions, empty());
    }

    @Test
    public void test_getMemberPartitionsIfAssigned_whenInitialized() {
        partitionService.firstArrangement();
        List<Integer> partitions = partitionService.getMemberPartitionsIfAssigned(getAddress(instance));
        assertEquals(partitionCount, partitions.size());
    }

    private static class TestPartitionListener implements PartitionListener {

        private int eventCount;

        @Override
        public void replicaChanged(PartitionReplicaChangeEvent event) {
            eventCount++;
        }
    }
}
