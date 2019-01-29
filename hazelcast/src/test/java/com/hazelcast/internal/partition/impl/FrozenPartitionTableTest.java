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
import com.hazelcast.nio.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class FrozenPartitionTableTest extends HazelcastTestSupport {

    @Test
    public void partitionTable_isFrozen_whenNodesLeave_duringClusterStateIsFrozen() {
        testPartitionTableIsFrozenDuring(ClusterState.FROZEN);
    }

    @Test
    public void partitionTable_isFrozen_whenNodesLeave_duringClusterStateIsPassive() {
        testPartitionTableIsFrozenDuring(ClusterState.PASSIVE);
    }

    @Test
    public void partitionTable_isFrozen_whenMemberReJoins_duringClusterStateIsFrozen() {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];
        HazelcastInstance hz3 = instances[2];
        Address hz3Address = getNode(hz3).getThisAddress();
        warmUpPartitions(instances);

        final Map<Integer, List<Address>> partitionTable = getPartitionTable(hz1);

        changeClusterStateEventually(hz2, ClusterState.FROZEN);

        terminateInstance(hz3);
        hz3 = factory.newHazelcastInstance(hz3Address);

        assertClusterSizeEventually(3, hz1, hz2, hz3);

        for (HazelcastInstance instance : Arrays.asList(hz1, hz2, hz3)) {
            final HazelcastInstance hz = instance;
            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {
                    assertPartitionTablesSame(partitionTable, getPartitionTable(hz));
                }
            });
        }
    }

    @Test
    public void partitionTable_shouldBeFixed_whenMemberLeaves_inFrozenState_thenStateChangesToActive() {
        testPartitionTableIsHealedWhenClusterStateIsActiveAfter(ClusterState.FROZEN);
    }

    @Test
    public void partitionTable_shouldBeFixed_whenMemberLeaves_inPassiveState_thenStateChangesToActive() {
        testPartitionTableIsHealedWhenClusterStateIsActiveAfter(ClusterState.PASSIVE);
    }

    private void testPartitionTableIsFrozenDuring(final ClusterState clusterState) {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();
        warmUpPartitions(instances);

        changeClusterStateEventually(instances[0], clusterState);
        List<HazelcastInstance> instancesList = new ArrayList<HazelcastInstance>(Arrays.asList(instances));
        Collections.shuffle(instancesList);

        final Map<Integer, List<Address>> partitionTable = getPartitionTable(instances[0]);

        while (instancesList.size() > 1) {
            final HazelcastInstance instanceToShutdown = instancesList.remove(0);
            instanceToShutdown.shutdown();
            for (HazelcastInstance instance : instancesList) {
                assertClusterSizeEventually(instancesList.size(), instance);
                assertPartitionTablesSame(partitionTable, getPartitionTable(instance));
            }
        }
    }

    private Map<Integer, List<Address>> getPartitionTable(final HazelcastInstance instance) {
        final InternalPartitionServiceImpl partitionService = getNode(instance).partitionService;
        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        final Map<Integer, List<Address>> partitionTable = new HashMap<Integer, List<Address>>();
        for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
            final InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
            for (int replicaIndex = 0; replicaIndex < InternalPartitionImpl.MAX_REPLICA_COUNT; replicaIndex++) {
                Address replicaAddress = partition.getReplicaAddress(replicaIndex);
                if (replicaAddress == null) {
                    break;
                }

                List<Address> replicaAddresses = partitionTable.get(partitionId);
                if (replicaAddresses == null) {
                    replicaAddresses = new ArrayList<Address>();
                    partitionTable.put(partitionId, replicaAddresses);
                }

                replicaAddresses.add(replicaAddress);
            }
        }

        return partitionTable;
    }

    private void testPartitionTableIsHealedWhenClusterStateIsActiveAfter(final ClusterState clusterState) {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();
        warmUpPartitions(instances);

        changeClusterStateEventually(instances[0], clusterState);

        List<HazelcastInstance> instancesList = new ArrayList<HazelcastInstance>(Arrays.asList(instances));
        Collections.shuffle(instancesList);
        final HazelcastInstance instanceToShutdown = instancesList.remove(0);
        final Address addressToShutdown = getNode(instanceToShutdown).getThisAddress();
        instanceToShutdown.shutdown();

        for (HazelcastInstance instance : instancesList) {
            assertClusterSizeEventually(2, instance);
        }

        instancesList.get(0).getCluster().changeClusterState(ClusterState.ACTIVE);
        waitAllForSafeState(instancesList);

        for (HazelcastInstance instance : instancesList) {
            final Map<Integer, List<Address>> partitionTable = getPartitionTable(instance);
            for (List<Address> addresses : partitionTable.values()) {
                for (Address address : addresses) {
                    assertNotEquals(addressToShutdown, address);
                }
            }
        }
    }

    private void assertPartitionTablesSame(Map<Integer, List<Address>> partitionTable1,
                                           Map<Integer, List<Address>> partitionTable2) {
        for (Map.Entry<Integer, List<Address>> partition : partitionTable1.entrySet()) {
            int partitionId = partition.getKey();
            List<Address> replicaAddresses1 = partition.getValue();
            List<Address> replicaAddresses2 = partitionTable2.get(partitionId);
            assertNotNull(replicaAddresses2);
            assertEquals(replicaAddresses1.size(), replicaAddresses2.size());
            for (int replicaIndex = 0; replicaIndex < replicaAddresses1.size(); replicaIndex++) {
                assertEquals(replicaAddresses1.get(replicaIndex), replicaAddresses2.get(replicaIndex));
            }
        }
    }

}
