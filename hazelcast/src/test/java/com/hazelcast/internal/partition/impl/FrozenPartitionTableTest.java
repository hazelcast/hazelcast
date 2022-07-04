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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.StaticMemberNodeContext;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.instance.impl.HazelcastInstanceFactory.newHazelcastInstance;
import static com.hazelcast.instance.impl.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.cluster.impl.ClusterJoinManager.STALE_JOIN_PREVENTION_DURATION_PROP;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static com.hazelcast.test.Accessors.getClusterService;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getOperationService;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.test.OverridePropertyRule.clear;
import static com.hazelcast.test.TestHazelcastInstanceFactory.initOrCreateConfig;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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

        final Member member3 = getClusterService(hz3).getLocalMember();

        terminateInstance(hz2);
        terminateInstance(hz3);

        HazelcastInstance newInstance = factory.newHazelcastInstance(hz3Address);
        hz3 = newInstance;
        Member newMember3 = getClusterService(hz3).getLocalMember();

        assertClusterSizeEventually(2, hz1, hz3);
        // ensure partition state is applied on the new member
        assertTrueEventually(() -> assertTrue(Accessors.isPartitionStateInitialized(newInstance)));

        final List<HazelcastInstance> instanceList = asList(hz1, hz3);
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instanceList) {
                    PartitionTableView newPartitionTable = getPartitionTable(instance);
                    for (int i = 0; i < newPartitionTable.length(); i++) {
                        for (int j = 0; j < InternalPartition.MAX_REPLICA_COUNT; j++) {
                            PartitionReplica replica = partitionTable.getReplica(i, j);
                            PartitionReplica newReplica = newPartitionTable.getReplica(i, j);

                            if (replica == null) {
                                assertNull(newReplica);
                            } else if (replica.equals(PartitionReplica.from(member3))) {
                                assertEquals(PartitionReplica.from(newMember3), newReplica);
                            } else {
                                assertEquals(replica, newReplica);
                            }
                        }
                    }
                }
            }
        }, 5);
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
            for (int i = 0; i < partitionTable.length(); i++) {
                for (PartitionReplica replica : partitionTable.getReplicas(i)) {
                    if (replica == null) {
                        continue;
                    }
                    assertNotEquals(addressToShutdown, replica.address());
                }
            }
        }
    }

    @Test
    public void partitionTable_shouldBeFixed_whenMemberRestarts_usingNewUuid() {
        ruleStaleJoinPreventionDuration.setOrClearProperty("5");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(3, hz2, hz3);
        warmUpPartitions(hz1, hz2, hz3);

        changeClusterStateEventually(hz3, ClusterState.FROZEN);
        int member3PartitionId = getPartitionId(hz3);

        MemberImpl member3 = getNode(hz3).getLocalMember();
        hz3.shutdown();
        assertClusterSizeEventually(2, hz1, hz2);

        hz3 = newHazelcastInstance(initOrCreateConfig(new Config()),
                randomName(), new StaticMemberNodeContext(factory, newUnsecureUUID(), member3.getAddress()));
        assertClusterSizeEventually(3, hz1, hz2);
        waitAllForSafeState(hz1, hz2, hz3);

        OperationServiceImpl operationService = getOperationService(hz1);
        operationService.invokeOnPartition(null, new NonRetryablePartitionOperation(), member3PartitionId).join();
    }

    @Test
    public void partitionTable_shouldBeFixed_whenMemberRestarts_usingUuidOfAnotherMissingMember() {
        ruleStaleJoinPreventionDuration.setOrClearProperty("5");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();
        HazelcastInstance hz4 = factory.newHazelcastInstance();

        assertClusterSizeEventually(4, hz2, hz3);
        warmUpPartitions(hz1, hz2, hz3, hz4);

        changeClusterStateEventually(hz4, ClusterState.FROZEN);
        int member3PartitionId = getPartitionId(hz3);
        int member4PartitionId = getPartitionId(hz4);

        MemberImpl member3 = getNode(hz3).getLocalMember();
        MemberImpl member4 = getNode(hz4).getLocalMember();
        hz3.shutdown();
        hz4.shutdown();
        assertClusterSizeEventually(2, hz1, hz2);

        newHazelcastInstance(initOrCreateConfig(new Config()),
                randomName(), new StaticMemberNodeContext(factory, member4.getUuid(), member3.getAddress()));
        assertClusterSizeEventually(3, hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        OperationServiceImpl operationService = getOperationService(hz1);
        operationService.invokeOnPartition(null, new NonRetryablePartitionOperation(), member3PartitionId).join();

        try {
            operationService.invokeOnPartition(null, new NonRetryablePartitionOperation(), member4PartitionId).joinInternal();
            fail("Invocation to missing member should have failed!");
        } catch (TargetNotMemberException ignored) {
        }
    }

    private static PartitionTableView getPartitionTable(HazelcastInstance instance) {
        return getPartitionService(instance).createPartitionTableView();
    }

    public static class NonRetryablePartitionOperation extends Operation {
        @Override
        public void run() throws Exception {
        }

        @Override
        public ExceptionAction onInvocationException(Throwable throwable) {
            if (throwable instanceof WrongTargetException || throwable instanceof TargetNotMemberException) {
                return ExceptionAction.THROW_EXCEPTION;
            }
            return super.onInvocationException(throwable);
        }
    }
}
