/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.cluster.impl.ClusterJoinManager.STALE_JOIN_PREVENTION_DURATION_PROP;
import static com.hazelcast.test.OverridePropertyRule.clear;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class FrozenPartitionTableTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule ruleStaleJoinPreventionDuration = clear(STALE_JOIN_PREVENTION_DURATION_PROP);

    @Test
    public void partitionTable_isFrozen_whenNodesLeave_duringClusterStateIsFrozen() {
        testPartitionTableIsFrozenDuring(ClusterState.FROZEN);
    }

    @Test
    public void partitionTable_isFrozen_whenNodesLeave_duringClusterStateIsPassive() {
        testPartitionTableIsFrozenDuring(ClusterState.PASSIVE);
    }

    private void testPartitionTableIsFrozenDuring(final ClusterState clusterState) {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();
        warmUpPartitions(instances);

        changeClusterStateEventually(instances[0], clusterState);
        List<HazelcastInstance> instancesList = new ArrayList<HazelcastInstance>(asList(instances));
        Collections.shuffle(instancesList);

        final PartitionTableView partitionTable = getPartitionTable(instances[0]);

        while (instancesList.size() > 1) {
            final HazelcastInstance instanceToShutdown = instancesList.remove(0);
            instanceToShutdown.shutdown();
            for (HazelcastInstance instance : instancesList) {
                assertClusterSizeEventually(instancesList.size(), instance);
                assertEquals(partitionTable, getPartitionTable(instance));
            }
        }
    }

    @Test
    public void partitionTable_isFrozen_whenMemberReJoins_duringClusterStateIsFrozen() {
        partitionTable_isFrozen_whenMemberReJoins_duringClusterStateIs(ClusterState.FROZEN);
    }

    @Test
    public void partitionTable_isFrozen_whenMemberReJoins_duringClusterStateIsPassive() {
        partitionTable_isFrozen_whenMemberReJoins_duringClusterStateIs(ClusterState.PASSIVE);
    }

    private void partitionTable_isFrozen_whenMemberReJoins_duringClusterStateIs(ClusterState state) {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];
        HazelcastInstance hz3 = instances[2];
        Address hz3Address = getNode(hz3).getThisAddress();
        warmUpPartitions(instances);

        final PartitionTableView partitionTable = getPartitionTable(hz1);

        changeClusterStateEventually(hz2, state);

        terminateInstance(hz2);
        terminateInstance(hz3);
        hz3 = factory.newHazelcastInstance(hz3Address);

        assertClusterSizeEventually(2, hz1, hz3);

        for (HazelcastInstance instance : asList(hz1, hz3)) {
            final HazelcastInstance hz = instance;
            AssertTask assertTask = new AssertTask() {
                @Override
                public void run() {
                    assertEquals(partitionTable, getPartitionTable(hz));
                }
            };
            assertTrueEventually(assertTask);
            assertTrueAllTheTime(assertTask, 3);
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

    private void testPartitionTableIsHealedWhenClusterStateIsActiveAfter(final ClusterState clusterState) {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();
        warmUpPartitions(instances);

        changeClusterStateEventually(instances[0], clusterState);

        List<HazelcastInstance> instancesList = new ArrayList<HazelcastInstance>(asList(instances));
        Collections.shuffle(instancesList);
        final HazelcastInstance instanceToShutdown = instancesList.remove(0);
        final Address addressToShutdown = getNode(instanceToShutdown).getThisAddress();
        instanceToShutdown.shutdown();

        for (HazelcastInstance instance : instancesList) {
            assertClusterSizeEventually(2, instance);
        }

        changeClusterStateEventually(instancesList.get(0), ClusterState.ACTIVE);
        waitAllForSafeState(instancesList);

        for (HazelcastInstance instance : instancesList) {
            PartitionTableView partitionTable = getPartitionTable(instance);
            for (int i = 0; i < partitionTable.getLength(); i++) {
                for (Address address : partitionTable.getAddresses(i)) {
                    assertNotEquals(addressToShutdown, address);
                }
            }
        }
    }

    private static PartitionTableView getPartitionTable(HazelcastInstance instance) {
        return getPartitionService(instance).createPartitionTableView();
    }
}
