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
 *
 */

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InternalPartitionServiceImplTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private InternalPartitionServiceImpl partitionService;
    private Address thisAddress;
    private int partitionCount;

    @Before
    public void setup() {
        instance = createHazelcastInstance();
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
    public void test_initialAssignment_whenClusterNotActive() {
        instance.getCluster().changeClusterState(ClusterState.FROZEN);

        partitionService.firstArrangement();
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

        partitionService.setInitialState(addresses, partitionCount);
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

        partitionService.setInitialState(addresses, 0);
        partitionService.setInitialState(addresses, 0);
    }

    @Test
    public void test_setInitialState_listenerShouldNOTBeCalled() {
        Address[][] addresses = new Address[partitionCount][MAX_REPLICA_COUNT];
        for (int i = 0; i < partitionCount; i++) {
            addresses[i][0] = thisAddress;
        }

        TestPartitionListener listener = new TestPartitionListener();
        partitionService.addPartitionListener(listener);

        partitionService.setInitialState(addresses, 0);
        assertEquals(0, listener.eventCount);
    }

    private static class TestPartitionListener implements PartitionListener {
        private int eventCount;

        @Override
        public void replicaChanged(PartitionReplicaChangeEvent event) {
            eventCount++;
        }
    }
}
