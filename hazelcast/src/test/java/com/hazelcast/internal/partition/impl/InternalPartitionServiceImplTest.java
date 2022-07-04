/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.partition.ReadonlyInternalPartition;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.mocknetwork.MockNodeContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getClusterService;
import static com.hazelcast.test.Accessors.getPartitionService;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InternalPartitionServiceImplTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private InternalPartitionServiceImpl partitionService;
    private Member localMember;
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
        localMember = getClusterService(instance).getLocalMember();
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
        assertEquals(0, partitionService.getPartitionStateStamp());
        assertNull(partitionService.getPartitionOwner(0));
    }

    @Test
    public void test_initialAssignment_whenClusterNotActive() {
        instance.getCluster().changeClusterState(ClusterState.FROZEN);

        partitionService.firstArrangement();

        assertFalse(partitionService.getPartitionStateManager().isInitialized());
        assertEquals(0, partitionService.getPartitionStateStamp());
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
        InternalPartition[] partitions = new InternalPartition[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            PartitionReplica[] replicas = new PartitionReplica[MAX_REPLICA_COUNT];
            replicas[0] = PartitionReplica.from(localMember);
            partitions[i] = new ReadonlyInternalPartition(replicas, i, RandomPicker.getInt(1, 10));
        }

        partitionService.setInitialState(new PartitionTableView(partitions));
        for (int i = 0; i < partitionCount; i++) {
            assertTrue(partitionService.isPartitionOwner(i));
        }
        assertNotEquals(0, partitionService.getPartitionStateStamp());
    }

    @Test(expected = IllegalStateException.class)
    public void test_setInitialState_multipleTimes() {
        InternalPartition[] partitions = new InternalPartition[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            PartitionReplica[] replicas = new PartitionReplica[MAX_REPLICA_COUNT];
            replicas[0] = PartitionReplica.from(localMember);
            partitions[i] = new ReadonlyInternalPartition(replicas, i, RandomPicker.getInt(1, 10));
        }

        partitionService.setInitialState(new PartitionTableView(partitions));
        partitionService.setInitialState(new PartitionTableView(partitions));
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
}
