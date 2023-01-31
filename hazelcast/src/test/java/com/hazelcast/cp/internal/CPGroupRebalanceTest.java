/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.internal.operation.GetLeadedGroupsOp;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.cp.internal.RaftGroupMembershipManager.LEADERSHIP_BALANCE_TASK_PERIOD;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPGroupRebalanceTest extends HazelcastRaftTestSupport {

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.setProperty(LEADERSHIP_BALANCE_TASK_PERIOD.getName(), String.valueOf(Integer.MAX_VALUE));
        return config;
    }

    @Test
    public void testDefaultRebalancing() throws Exception {
        int groupSize = 5;
        int leadershipsPerMember = 10;
        int groupCount = groupSize * leadershipsPerMember - 1;

        HazelcastInstance[] instances = newInstances(groupSize, groupSize, 0);
        waitUntilCPDiscoveryCompleted(instances);

        Collection<CPGroupId> groupIds = new ArrayList<>(groupCount);
        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        for (int i = 0; i < groupCount; i++) {
            RaftGroupId groupId = invocationManager.createRaftGroup("group-" + i).joinInternal();
            groupIds.add(groupId);
        }

        HazelcastInstance metadataLeader = getLeaderInstance(instances, getMetadataGroupId(instances[0]));
        Collection<CPMember> cpMembers =
                metadataLeader.getCPSubsystem().getCPSubsystemManagementService().getCPMembers().toCompletableFuture().get();

        // Assert eventually since during the test
        // a long pause can cause leadership change unexpectedly.
        assertTrueEventually(() -> {
            // Wait for leader election all groups
            for (CPGroupId groupId : groupIds) {
                waitAllForLeaderElection(instances, groupId);
            }

            rebalanceLeadership(instances);

            Map<CPMember, Collection<CPGroupId>> leadershipsMap = getLeadershipsMap(metadataLeader, cpMembers);
            for (Entry<CPMember, Collection<CPGroupId>> entry : leadershipsMap.entrySet()) {
                int count = entry.getValue().size();
                assertEquals(leadershipsString(leadershipsMap), leadershipsPerMember, count);
            }
        });
    }

    @Test
    public void testRebalancingWhenGroupSizeLessThenCPMembers() {
        int groupCount = 10;

        Config config = createConfig(5, 3);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        config.getCPSubsystemConfig().setCPMemberPriority(-1);
        HazelcastInstance instance4 = factory.newHazelcastInstance(config);
        HazelcastInstance instance5 = factory.newHazelcastInstance(config);

        HazelcastInstance[] instances = {instance1, instance2, instance3, instance4, instance5};
        waitUntilCPDiscoveryCompleted(instances);
        List<HazelcastInstance> cpMemberList = Arrays.asList(
                instance1,
                instance2,
                instance3
        );

        RaftInvocationManager invocationManager = getRaftInvocationManager(instance1);

        Collection<CPGroupId> groupIds = new ArrayList<>();
        RaftGroupId metadataGroupId = getMetadataGroupId(instance1);
        groupIds.add(metadataGroupId);
        for (int i = 0; i < groupCount; i++) {
            RaftGroupId groupId = invocationManager.createRaftGroup("group-" + i).joinInternal();
            groupIds.add(groupId);
            HazelcastInstance leaderInstance = getLeaderInstance(instances, groupId);
            assertTrueEventually(() -> assertNotNull(leaderInstance));
        }

        assertTrueEventually(() -> {
            rebalanceLeadership(instances);

            for (CPGroupId groupId : groupIds) {
                HazelcastInstance leaderInstance = getLeaderInstance(instances, groupId);
                assertContains(cpMemberList, leaderInstance);
            }
        });
    }

    @Test
    public void testRebalancingWithOneNonPriorityMember() throws Exception {
        int groupSize = 5;
        int leadershipsPerMember = 12;
        int groupCount = (groupSize - 1) * leadershipsPerMember - 1;

        Config config = createConfig(5, 5);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        HazelcastInstance instance4 = factory.newHazelcastInstance(config);
        config.getCPSubsystemConfig().setCPMemberPriority(-1);
        HazelcastInstance instance5 = factory.newHazelcastInstance(config);

        HazelcastInstance[] instances = {instance1, instance2, instance3, instance4, instance5};
        waitUntilCPDiscoveryCompleted(instances);

        Collection<CPGroupId> groupIds = new ArrayList<>(groupCount);
        RaftInvocationManager invocationManager = getRaftInvocationManager(instance1);
        for (int i = 0; i < groupCount; i++) {
            RaftGroupId groupId = invocationManager.createRaftGroup("group-" + i).joinInternal();
            groupIds.add(groupId);
        }

        HazelcastInstance metadataLeader = getLeaderInstance(instances, getMetadataGroupId(instance1));
        Collection<CPMember> cpMembers =
                metadataLeader.getCPSubsystem().getCPSubsystemManagementService().getCPMembers().toCompletableFuture().get();

        // Assert eventually since during the test
        // a long pause can cause leadership change unexpectedly.
        assertTrueEventually(() -> {
            // Wait for leader election all groups
            for (CPGroupId groupId : groupIds) {
                waitAllForLeaderElection(instances, groupId);
            }

            rebalanceLeadership(instances);

            Map<CPMember, Collection<CPGroupId>> leadershipsMap = getLeadershipsMap(metadataLeader, cpMembers);
            for (Entry<CPMember, Collection<CPGroupId>> entry : leadershipsMap.entrySet()) {
                int count = entry.getValue().size();
                if (entry.getKey().getUuid().equals(instance5.getLocalEndpoint().getUuid())) {
                    assertEquals(leadershipsString(leadershipsMap), 0, count);
                } else {
                    assertEquals(leadershipsString(leadershipsMap), leadershipsPerMember, count);
                }
            }
        });
    }

    @Test
    public void testRebalancingWithTwoNonPriorityMembers() throws Exception {
        int groupSize = 5;
        int leadershipsPerMember = 15;
        int groupCount = (groupSize - 2) * leadershipsPerMember - 1;

        Config config = createConfig(5, 5);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        config.getCPSubsystemConfig().setCPMemberPriority(-1);
        HazelcastInstance instance4 = factory.newHazelcastInstance(config);
        HazelcastInstance instance5 = factory.newHazelcastInstance(config);

        HazelcastInstance[] instances = {instance1, instance2, instance3, instance4, instance5};
        waitUntilCPDiscoveryCompleted(instances);

        Collection<CPGroupId> groupIds = new ArrayList<>(groupCount);
        RaftInvocationManager invocationManager = getRaftInvocationManager(instance1);
        for (int i = 0; i < groupCount; i++) {
            RaftGroupId groupId = invocationManager.createRaftGroup("group-" + i).joinInternal();
            groupIds.add(groupId);
        }

        HazelcastInstance metadataLeader = getLeaderInstance(instances, getMetadataGroupId(instance1));
        Collection<CPMember> cpMembers =
                metadataLeader.getCPSubsystem().getCPSubsystemManagementService().getCPMembers().toCompletableFuture().get();

        // Assert eventually since during the test
        // a long pause can cause leadership change unexpectedly.
        assertTrueEventually(() -> {
            // Wait for leader election all groups
            for (CPGroupId groupId : groupIds) {
                waitAllForLeaderElection(instances, groupId);
            }

            rebalanceLeadership(instances);

            Map<CPMember, Collection<CPGroupId>> leadershipsMap = getLeadershipsMap(metadataLeader, cpMembers);
            for (Entry<CPMember, Collection<CPGroupId>> entry : leadershipsMap.entrySet()) {
                int count = entry.getValue().size();
                if (entry.getKey().getUuid().equals(instance4.getLocalEndpoint().getUuid())
                        || entry.getKey().getUuid().equals(instance5.getLocalEndpoint().getUuid())) {
                    assertEquals(leadershipsString(leadershipsMap), 0, count);
                } else {
                    assertEquals(leadershipsString(leadershipsMap), leadershipsPerMember, count);
                }
            }
        });
    }

    @Test
    public void testDefaultGroupTransferring() throws Exception {
        Config config = createConfig(3, 3);

        config.getCPSubsystemConfig().setCPMemberPriority(1);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        config.getCPSubsystemConfig().setCPMemberPriority(2);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        config.getCPSubsystemConfig().setCPMemberPriority(3);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);

        HazelcastInstance[] instances = {instance1, instance2, instance3};
        waitUntilCPDiscoveryCompleted(instances);

        IAtomicLong atomicLong = instance1.getCPSubsystem().getAtomicLong("atomic-long");
        assertEquals(1L, atomicLong.incrementAndGet());
        CPGroup cpGroup = getRaftService(instance1).getCPGroup(CPGroup.DEFAULT_GROUP_NAME).get();
        waitAllForLeaderElection(instances, cpGroup.id());
        waitAllForLeaderElection(instances, getMetadataGroupId(instance1));

        rebalanceLeadership(instances);

        RaftNodeImpl metadataGroupLeader = waitAllForLeaderElection(instances, getMetadataGroupId(instance1));
        HazelcastInstance metadataGroupLeaderIns = getInstance(metadataGroupLeader.getLeader());
        RaftNodeImpl defaultGroupLeader = waitAllForLeaderElection(instances, cpGroup.id());
        HazelcastInstance defaultGroupLeaderIns = getInstance(defaultGroupLeader.getLeader());

        assertEquals(instance3, metadataGroupLeaderIns);
        assertEquals(instance3, defaultGroupLeaderIns);
    }

    @Test
    public void testTransferringWithSeveralPriorityMembers() throws Exception {
        Config config = createConfig(3, 3);

        config.getCPSubsystemConfig().setCPMemberPriority(1);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        config.getCPSubsystemConfig().setCPMemberPriority(2);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        config.getCPSubsystemConfig().setCPMemberPriority(2);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);

        HazelcastInstance[] instances = {instance1, instance2, instance3};
        waitUntilCPDiscoveryCompleted(instances);

        IAtomicLong atomicLong = instance1.getCPSubsystem().getAtomicLong("atomic-long");
        assertEquals(1L, atomicLong.incrementAndGet());
        CPGroup cpGroup = getRaftService(instance1).getCPGroup(CPGroup.DEFAULT_GROUP_NAME).get();
        waitAllForLeaderElection(instances, cpGroup.id());
        waitAllForLeaderElection(instances, getMetadataGroupId(instance1));

        rebalanceLeadership(instances);

        RaftNodeImpl metadataGroupLeader = waitAllForLeaderElection(instances, getMetadataGroupId(instance1));
        HazelcastInstance metadataGroupLeaderIns = getInstance(metadataGroupLeader.getLeader());
        RaftNodeImpl defaultGroupLeader = waitAllForLeaderElection(instances, cpGroup.id());
        HazelcastInstance defaultGroupLeaderIns = getInstance(defaultGroupLeader.getLeader());

        assertTrue((metadataGroupLeaderIns.equals(instance2)) || (metadataGroupLeaderIns.equals(instance3)));
        assertTrue((defaultGroupLeaderIns.equals(instance2)) || (defaultGroupLeaderIns.equals(instance3)));
    }

    @Test
    public void testTransferringWhenCPMembersFailed() throws Exception {
        Config config = createConfig(3, 3);

        config.getCPSubsystemConfig().setCPMemberPriority(1);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        config.getCPSubsystemConfig().setCPMemberPriority(2);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        config.getCPSubsystemConfig().setCPMemberPriority(3);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);

        HazelcastInstance[] instances = {instance1, instance2, instance3};
        HazelcastInstance[] instances2 = {instance1, instance2};
        waitUntilCPDiscoveryCompleted(instances);

        IAtomicLong atomicLong = instance1.getCPSubsystem().getAtomicLong("atomic-long");
        CPGroup cpGroup = getRaftService(instance1).getCPGroup(CPGroup.DEFAULT_GROUP_NAME).get();
        assertEquals(1L, atomicLong.incrementAndGet());

        instance3.getLifecycleService().terminate();
        assertClusterSizeEventually(2, instances2);

        waitAllForLeaderElection(instances2, cpGroup.id());
        waitAllForLeaderElection(instances2, getMetadataGroupId(instance1));

        getRaftService(instance1).transferLeadership(cpGroup.id(), getRaftService(instance1).getLocalCPMember());
        getRaftService(instance1).transferLeadership(getMetadataGroupId(instance1), getRaftService(instance1).getLocalCPMember());

        waitAllForLeaderElection(instances2, cpGroup.id());
        waitAllForLeaderElection(instances2, getMetadataGroupId(instance1));

        rebalanceLeadership(instances2);

        RaftNodeImpl metadataGroupLeader = waitAllForLeaderElection(instances2, getMetadataGroupId(instance1));
        HazelcastInstance metadataGroupLeaderIns = getInstance(metadataGroupLeader.getLeader());
        RaftNodeImpl defaultGroupLeader = waitAllForLeaderElection(instances2, cpGroup.id());
        HazelcastInstance defaultGroupLeaderIns = getInstance(defaultGroupLeader.getLeader());

        assertEquals(instance2, metadataGroupLeaderIns);
        assertEquals(instance2, defaultGroupLeaderIns);
    }

    @Test
    public void testTransferringWhenCPMembersChanged() throws Exception {
        Config config = createConfig(3, 3);

        config.getCPSubsystemConfig().setCPMemberPriority(1);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        config.getCPSubsystemConfig().setCPMemberPriority(2);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        config.getCPSubsystemConfig().setCPMemberPriority(3);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);

        HazelcastInstance[] instances = {instance1, instance2, instance3};
        HazelcastInstance[] instances2 = {instance1, instance2};
        waitUntilCPDiscoveryCompleted(instances);

        IAtomicLong atomicLong = instance1.getCPSubsystem().getAtomicLong("atomic-long");
        CPGroup cpGroup = getRaftService(instance1).getCPGroup(CPGroup.DEFAULT_GROUP_NAME).get();
        assertEquals(1L, atomicLong.incrementAndGet());

        instance3.shutdown();
        assertClusterSizeEventually(2, instances2);

        waitAllForLeaderElection(instances2, cpGroup.id());
        waitAllForLeaderElection(instances2, getMetadataGroupId(instance1));

        rebalanceLeadership(instances2);

        RaftNodeImpl metadataGroupLeader = waitAllForLeaderElection(instances2, getMetadataGroupId(instance1));
        HazelcastInstance metadataGroupLeaderIns = getInstance(metadataGroupLeader.getLeader());
        RaftNodeImpl defaultGroupLeader = waitAllForLeaderElection(instances2, cpGroup.id());
        HazelcastInstance defaultGroupLeaderIns = getInstance(defaultGroupLeader.getLeader());

        assertEquals(instance2, metadataGroupLeaderIns);
        assertEquals(instance2, defaultGroupLeaderIns);

        config.getCPSubsystemConfig().setCPMemberPriority(4);
        HazelcastInstance instance4 = factory.newHazelcastInstance(config);
        HazelcastInstance[] instances3 = {instance1, instance2, instance4};

        instance4.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                .toCompletableFuture().get();
        waitUntilCPDiscoveryCompleted(instances3);

        waitAllForLeaderElection(instances3, cpGroup.id());
        waitAllForLeaderElection(instances3, getMetadataGroupId(instance1));

        rebalanceLeadership(instances3);

        RaftNodeImpl metadataGroupLeaderNew = waitAllForLeaderElection(instances3, getMetadataGroupId(instance1));
        HazelcastInstance metadataGroupLeaderInsNew = getInstance(metadataGroupLeaderNew.getLeader());
        RaftNodeImpl defaultGroupLeaderNew = waitAllForLeaderElection(instances3, cpGroup.id());
        HazelcastInstance defaultGroupLeaderInsNew = getInstance(defaultGroupLeaderNew.getLeader());

        assertEquals(instance4, metadataGroupLeaderInsNew);
        assertEquals(instance4, defaultGroupLeaderInsNew);
    }

    private void rebalanceLeadership(HazelcastInstance[] instances) {
        HazelcastInstance metadataLeader = getLeaderInstance(instances, getMetadataGroupId(instances[0]));
        getRaftService(metadataLeader).getMetadataGroupManager().rebalanceGroupLeaderships();
    }

    private String leadershipsString(Map<CPMember, Collection<CPGroupId>> leadershipsMap) {
        StringBuilder s = new StringBuilder("====== LEADERSHIPS ======\n");
        for (Entry<CPMember, Collection<CPGroupId>> entry : leadershipsMap.entrySet()) {
            s.append(entry.getKey()).append(" => ").append(entry.getValue().size()).append('\n');
        }
        return s.toString();
    }

    private Map<CPMember, Collection<CPGroupId>> getLeadershipsMap(HazelcastInstance instance, Collection<CPMember> members) {
        OperationServiceImpl operationService = getOperationService(instance);
        Map<CPMember, Collection<CPGroupId>> leaderships = new HashMap<>();

        for (CPMember member : members) {
            Entry<Integer, Collection<CPGroupId>> entry =
                    operationService.<Entry<Integer, Collection<CPGroupId>>>invokeOnTarget(RaftService.SERVICE_NAME, new GetLeadedGroupsOp(), member.getAddress())
                            .join();
            leaderships.put(member, entry.getValue());
        }
        return leaderships;
    }
}
