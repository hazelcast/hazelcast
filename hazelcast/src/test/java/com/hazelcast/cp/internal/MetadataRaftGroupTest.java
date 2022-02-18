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

package com.hazelcast.cp.internal;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.operation.ResetCPMemberOp;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.cp.CPGroup.DEFAULT_GROUP_NAME;
import static com.hazelcast.cp.internal.MetadataRaftGroupManager.MetadataRaftGroupInitStatus.IN_PROGRESS;
import static com.hazelcast.cp.internal.MetadataRaftGroupManager.MetadataRaftGroupInitStatus.SUCCESSFUL;
import static com.hazelcast.cp.internal.RaftServiceDataSerializerHook.APPEND_REQUEST_OP;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.FINALIZE_JOIN;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.F_ID;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsToAddresses;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetadataRaftGroupTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;

    @Test
    public void when_clusterStartsWithNonCPNodes_then_metadataClusterIsInitialized() {
        int cpNodeCount = 3;
        instances = newInstances(cpNodeCount, cpNodeCount, 2);

        List<Address> raftAddresses = new ArrayList<>();
        for (int i = 0; i < cpNodeCount; i++) {
            raftAddresses.add(getAddress(instances[i]));
        }

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                if (raftAddresses.contains(getAddress(instance))) {
                    assertNotNull(getRaftNode(instance, getMetadataGroupId(instance)));
                }
            }
        });

        assertTrueAllTheTime(() -> {
            for (HazelcastInstance instance : instances) {
                if (!raftAddresses.contains(getAddress(instance))) {
                    assertNull(getRaftNode(instance, getMetadataGroupId(instance)));
                }
            }
        }, 10);
    }

    @Test
    public void when_clusterStartsWithCPNodes_then_CPDiscoveryCompleted()  {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                MetadataRaftGroupManager metadataGroupManager = getRaftService(instance).getMetadataGroupManager();
                assertEquals(SUCCESSFUL, metadataGroupManager.getInitializationStatus());
                assertEquals(nodeCount, metadataGroupManager.getActiveMembers().size());
                assertNotNull(metadataGroupManager.getInitialCPMembers());
                assertEquals(nodeCount, metadataGroupManager.getInitialCPMembers().size());
                assertTrue(metadataGroupManager.getInitializedCPMembers().isEmpty());
                assertTrue(metadataGroupManager.getInitializationCommitIndices().isEmpty());
            }
        });
    }

    @Test
    public void when_slaveMissesItsJoinResponse_then_CPDiscoveryCompleted() throws ExecutionException, InterruptedException {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);

        HazelcastInstance master = factory.newHazelcastInstance(config);
        HazelcastInstance slave1 = factory.newHazelcastInstance(config);

        Address slaveAddress2 = factory.nextAddress();

        dropOperationsToAddresses(master, singletonList(slaveAddress2), F_ID, singletonList(FINALIZE_JOIN));

        Future<HazelcastInstance> f = spawn(() -> factory.newHazelcastInstance(slaveAddress2, config));

        assertClusterSizeEventually(3, master);

        assertTrueEventually(() -> {
            MetadataRaftGroupManager metadataGroupManager = getRaftService(master).getMetadataGroupManager();
            assertNotNull(metadataGroupManager.getInitialCPMembers());
            assertEquals(3, metadataGroupManager.getInitialCPMembers().size());
            assertEquals(2, metadataGroupManager.getInitializedCPMembers().size());
        });

        assertTrueAllTheTime(() -> {
            MetadataRaftGroupManager metadataGroupManager = getRaftService(master).getMetadataGroupManager();
            assertEquals(IN_PROGRESS, metadataGroupManager.getInitializationStatus());
            assertEquals(2, metadataGroupManager.getInitializedCPMembers().size());
        }, 10);

        resetPacketFiltersFrom(master);

        HazelcastInstance slave2 = f.get();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : Arrays.asList(master, slave1, slave2)) {
                RaftService service = getRaftService(instance);
                assertTrue(service.getMetadataGroupManager().isDiscoveryCompleted());
            }
        });
    }

    @Test
    public void when_raftGroupIsCreatedWithAllCPNodes_then_raftNodeIsCreatedOnAll()  {
        int nodeCount = 5;
        instances = newInstances(nodeCount);

        CPGroupId groupId = createNewRaftGroup(instances[0], "id", nodeCount);

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                assertNotNull(getRaftNode(instance, groupId));
            }
        });
    }

    @Test
    public void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyThem() {
        int nodeCount = 5;
        int metadataGroupSize = 3;
        instances = newInstances(nodeCount, metadataGroupSize, 0);

        List<Member> raftMembers = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            raftMembers.add(instances[i].getCluster().getLocalMember());
        }

        raftMembers.sort(Comparator.comparing(Member::getUuid));

        assertTrueEventually(() -> {
            for (Member member : raftMembers.subList(0, metadataGroupSize)) {
                HazelcastInstance instance = factory.getInstance(member.getAddress());
                assertNotNull(getRaftNode(instance, getMetadataGroupId(instance)));
            }
        });

        assertTrueAllTheTime(() -> {
            for (Member member : raftMembers.subList(metadataGroupSize, raftMembers.size())) {
                HazelcastInstance instance = factory.getInstance(member.getAddress());
                assertNull(getRaftNode(instance, getMetadataGroupId(instance)));
            }
        }, 10);
    }

    @Test
    public void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints() {
        when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints(true);
    }

    @Test
    public void when_raftGroupIsCreatedFromNonCPNode_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints() {
        when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints(false);
    }

    private void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyTheSelectedEndpoints(boolean invokeOnCP) {
        int cpNodeCount = 6;
        int metadataGroupSize = 3;
        int nonCpNodeCount = 2;
        instances = newInstances(cpNodeCount, metadataGroupSize, nonCpNodeCount);

        int newGroupCount = metadataGroupSize + 1;

        HazelcastInstance instance = instances[invokeOnCP ? 0 : instances.length - 1];
        CPGroupId groupId = createNewRaftGroup(instance, "id", newGroupCount);

        assertTrueEventually(() -> {
            int count = 0;
            for (HazelcastInstance instance1 : instances) {
                RaftNodeImpl raftNode = getRaftNode(instance1, groupId);
                if (raftNode != null) {
                    count++;
                }
            }

            assertEquals(newGroupCount, count);
        });
    }

    @Test
    public void when_sizeOfRaftGroupIsLargerThanCPNodeCount_then_raftGroupCannotBeCreated() {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        try {
            createNewRaftGroup(instances[0], "id", nodeCount + 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void when_raftGroupIsCreatedWithSameSizeMultipleTimes_then_itSucceeds() {
        int nodeCount = 3;
        instances = newInstances(nodeCount);
        CPGroupId groupId1 = createNewRaftGroup(instances[0], "id", nodeCount);
        CPGroupId groupId2 = createNewRaftGroup(instances[1], "id", nodeCount);
        assertEquals(groupId1, groupId2);
    }

    @Test
    public void when_raftGroupIsCreatedWithDifferentSizeMultipleTimes_then_itFails() {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        createNewRaftGroup(instances[0], "id", nodeCount);
        try {
            createNewRaftGroup(instances[0], "id", nodeCount - 1);
            fail();
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void when_raftGroupDestroyTriggered_then_raftGroupIsDestroyed() throws ExecutionException, InterruptedException {
        int metadataGroupSize = 3;
        int cpNodeCount = 5;
        instances = newInstances(cpNodeCount, metadataGroupSize, 0);

        CPGroupId groupId = createNewRaftGroup(instances[0], "id", cpNodeCount);

        CPGroup group = instances[0].getCPSubsystem().getCPSubsystemManagementService().getCPGroup("id")
                                    .toCompletableFuture().get();
        assertNotNull(group);
        assertEquals(groupId, group.id());

        destroyRaftGroup(instances[0], groupId);

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                assertNull(getRaftNode(instance, groupId));
            }
        });

        assertTrueEventually(() -> {
            CPGroup g = getRaftService(instances[0]).getCPGroup(groupId).get();
            assertEquals(CPGroupStatus.DESTROYED, g.status());
        });
    }

    @Test
    public void when_raftGroupDestroyTriggeredMultipleTimes_then_destroyDoesNotFail() {
        int metadataGroupSize = 3;
        int cpNodeCount = 5;
        instances = newInstances(cpNodeCount, metadataGroupSize, 0);

        CPGroupId groupId = createNewRaftGroup(instances[0], "id", cpNodeCount);
        destroyRaftGroup(instances[0], groupId);
        destroyRaftGroup(instances[0], groupId);
    }

    @Test
    public void when_raftGroupIsDestroyed_then_itCanBeCreatedAgain() {
        int metadataGroupSize = 3;
        int cpNodeCount = 5;
        instances = newInstances(cpNodeCount, metadataGroupSize, 0);

        CPGroupId groupId = createNewRaftGroup(instances[0], "id", cpNodeCount);

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                assertNotNull(getRaftNode(instance, groupId));
            }
        });

        destroyRaftGroup(instances[0], groupId);

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                assertNull(getRaftNode(instance, groupId));
            }
        });

        createNewRaftGroup(instances[0], "id", cpNodeCount - 1);
    }

    @Test
    public void when_nonMetadataRaftGroupIsAlive_then_itCanBeForceDestroyed() throws ExecutionException, InterruptedException {
        instances = newInstances(3);

        waitAllForLeaderElection(instances, getMetadataGroupId(instances[0]));

        CPGroupId groupId = createNewRaftGroup(instances[0], "id", 3);

        CPGroup group = instances[0].getCPSubsystem().getCPSubsystemManagementService().getCPGroup("id")
                                    .toCompletableFuture().get();
        assertNotNull(group);
        assertEquals(groupId, group.id());

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                assertNotNull(getRaftService(instance).getRaftNode(groupId));
            }
        });

        getRaftService(instances[0]).forceDestroyCPGroup(groupId.getName()).get();

        group = getRaftService(instances[0]).getCPGroup(groupId).get();
        assertEquals(CPGroupStatus.DESTROYED, group.status());

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                assertNull(getRaftNode(instance, groupId));
            }
        });

        CPGroupId newGroupId = createNewRaftGroup(instances[0], "id", 3);
        assertNotEquals(groupId, newGroupId);
    }

    @Test
    public void when_nonMetadataRaftGroupLosesMajority_then_itCanBeForceDestroyed() throws ExecutionException, InterruptedException {
        instances = newInstances(5);

        waitAllForLeaderElection(instances, getMetadataGroupId(instances[0]));

        CPGroupId groupId = createNewRaftGroup(instances[0], "id", 3);

        CPGroup group = instances[0].getCPSubsystem().getCPSubsystemManagementService().getCPGroup("id")
                                    .toCompletableFuture().get();
        assertNotNull(group);
        assertEquals(groupId, group.id());

        CPMember[] groupMembers = group.members().toArray(new CPMember[0]);

        assertTrueEventually(() -> {
            for (CPMember member : groupMembers) {
                HazelcastInstance instance = factory.getInstance(member.getAddress());
                assertNotNull(getRaftNode(instance, groupId));
            }
        });

        factory.getInstance(groupMembers[0].getAddress()).getLifecycleService().terminate();
        factory.getInstance(groupMembers[1].getAddress()).getLifecycleService().terminate();

        HazelcastInstance runningInstance = factory.getInstance(groupMembers[2].getAddress());
        getRaftService(runningInstance).forceDestroyCPGroup(groupId.getName()).get();

        group = getRaftService(runningInstance).getCPGroup(groupId).get();
        assertEquals(CPGroupStatus.DESTROYED, group.status());

        assertTrueEventually(() -> assertNull(getRaftNode(runningInstance, groupId)));
    }

    @Test
    public void when_metadataClusterNodeFallsFarBehind_then_itInstallsSnapshot() {
        int nodeCount = 3;
        int commitCountToSnapshot = 5;
        Config config = createConfig(nodeCount, nodeCount);
        config.getCPSubsystemConfig().getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(commitCountToSnapshot);

        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }

        waitUntilCPDiscoveryCompleted(instances);

        waitAllForLeaderElection(instances, getMetadataGroupId(instances[0]));
        RaftEndpoint leaderEndpoint = getLeaderMember(getRaftNode(instances[0], getMetadataGroupId(instances[0])));

        HazelcastInstance leader = getInstance(leaderEndpoint);
        HazelcastInstance follower = getRandomFollowerInstance(instances, getMetadataGroupId(instances[0]));

        dropOperationsBetween(leader, follower, RaftServiceDataSerializerHook.F_ID, singletonList(APPEND_REQUEST_OP));

        List<CPGroupId> groupIds = new ArrayList<>();
        for (int i = 0; i < commitCountToSnapshot; i++) {
            CPGroupId groupId = createNewRaftGroup(leader, "id" + i, nodeCount);
            groupIds.add(groupId);
        }

        assertTrueEventually(() -> assertTrue(getSnapshotEntry(getRaftNode(leader, getMetadataGroupId(leader))).index() > 0));

        assertTrueEventually(() -> {
            for (CPGroupId groupId : groupIds) {
                assertNotNull(getRaftNode(follower, groupId));
            }
        });
    }

    @Test
    public void when_raftGroupIsCreated_onNonMetadataMembers_thenLeaderShouldBeElected() throws ExecutionException, InterruptedException {
        int metadataGroupSize = 3;
        int otherRaftGroupSize = 2;
        instances = newInstances(metadataGroupSize + otherRaftGroupSize, metadataGroupSize, 0);

        HazelcastInstance leaderInstance = getLeaderInstance(instances, getMetadataGroupId(instances[0]));
        RaftService raftService = getRaftService(leaderInstance);
        Collection<CPMemberInfo> allEndpoints = raftService.getMetadataGroupManager().getActiveMembers();

        assertTrueEventually(
                () -> assertNotNull(raftService.getMetadataGroupManager().getGroup(getMetadataGroupId(leaderInstance))));
        CPGroup metadataGroup = raftService.getMetadataGroupManager().getGroup(getMetadataGroupId(leaderInstance));

        Collection<CPMemberInfo> endpoints = new HashSet<>(otherRaftGroupSize);
        for (CPMemberInfo endpoint : allEndpoints) {
            if (!metadataGroup.members().contains(endpoint)) {
                endpoints.add(endpoint);
            }
        }
        assertEquals(otherRaftGroupSize, endpoints.size());

        List<RaftEndpoint> groupEndpoints = new ArrayList<>();
        for (CPMemberInfo member : endpoints) {
            groupEndpoints.add(member.toRaftEndpoint());
        }

        RaftOp op = new CreateRaftGroupOp("test", groupEndpoints, RandomPicker.getInt(Integer.MAX_VALUE));
        InternalCompletableFuture<CPGroupSummary> f = raftService.getInvocationManager()
                                                                 .invoke(getMetadataGroupId(leaderInstance), op);

        f.whenCompleteAsync((group, t) -> {
            if (t == null) {
                raftService.getInvocationManager().triggerRaftNodeCreation(group);
            }
        });

        CPGroupId groupId = f.get().id();

        for (HazelcastInstance instance : instances) {
            if (endpoints.contains(instance.getCPSubsystem().getLocalCPMember())) {
                assertTrueEventually(() -> {
                    RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                    assertNotNull(raftNode);
                    assertNotNull("Leader is null on " + raftNode, getLeaderMember(raftNode));
                });
            }
        }
    }

    @Test
    public void when_shutdownLeader_thenNewLeaderElected() {
        int cpNodeCount = 6;
        int metadataGroupSize = 5;
        instances = newInstances(cpNodeCount, metadataGroupSize, 0);

        HazelcastInstance leaderInstance = getLeaderInstance(instances, getMetadataGroupId(instances[0]));
        leaderInstance.shutdown();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                if (instance == leaderInstance) {
                    continue;
                }
                RaftNodeImpl raftNode = getRaftNode(instance, getMetadataGroupId(instance));
                assertNotNull(raftNode);
                assertNotNull("Leader is null on " + raftNode, getLeaderMember(raftNode));
            }
        });
    }

    @Test
    public void when_memberIsShutdown_then_itIsRemovedFromRaftGroups() throws ExecutionException, InterruptedException {
        int cpNodeCount = 7;
        int metadataGroupSize = 5;
        int atomicLong1GroupSize = 3;
        instances = newInstances(cpNodeCount, metadataGroupSize, 0);

        CPGroupId groupId1 = createNewRaftGroup(instances[0], "id1", atomicLong1GroupSize);
        CPGroupId groupId2 = createNewRaftGroup(instances[0], "id2", cpNodeCount);

        CPMember endpoint = findCommonEndpoint(instances[0], getMetadataGroupId(instances[0]), groupId1);
        assertNotNull(endpoint);

        RaftInvocationManager invocationService = null;
        HazelcastInstance aliveInstance = null;
        for (HazelcastInstance instance : instances) {
            if (!getAddress(instance).equals(endpoint.getAddress())) {
                aliveInstance = instance;
                invocationService = getRaftInvocationManager(instance);
                break;
            }
        }
        assertNotNull(invocationService);

        factory.getInstance(endpoint.getAddress()).shutdown();

        CPGroupId metadataGroupId = getMetadataGroupId(aliveInstance);
        InternalCompletableFuture<List<CPMember>> f1 = invocationService.query(metadataGroupId, new GetActiveCPMembersOp(),
                LINEARIZABLE);

        List<CPMember> activeEndpoints = f1.get();
        assertThat(activeEndpoints, not(hasItem(endpoint)));

        InternalCompletableFuture<CPGroup> f2 = invocationService.query(metadataGroupId, new GetRaftGroupOp(metadataGroupId),
                LINEARIZABLE);

        InternalCompletableFuture<CPGroup> f3 = invocationService.query(metadataGroupId, new GetRaftGroupOp(groupId1),
                LINEARIZABLE);

        InternalCompletableFuture<CPGroup> f4 = invocationService.query(metadataGroupId, new GetRaftGroupOp(groupId2),
                LINEARIZABLE);

        CPGroup metadataGroup = f2.get();
        assertFalse(metadataGroup.members().contains(endpoint));
        assertEquals(metadataGroupSize, metadataGroup.members().size());
        CPGroup atomicLongGroup1 = f3.get();
        assertFalse(atomicLongGroup1.members().contains(endpoint));
        assertEquals(atomicLong1GroupSize, atomicLongGroup1.members().size());
        CPGroup atomicLongGroup2 = f4.get();
        assertFalse(atomicLongGroup2.members().contains(endpoint));
        assertEquals(cpNodeCount - 1, atomicLongGroup2.members().size());
    }

    @Test
    public void when_nonReachableEndpointsExist_createRaftGroupPrefersReachableEndpoints()
            throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(5);

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                Collection<CPMemberInfo> raftMembers = getRaftService(instance).getMetadataGroupManager().getActiveMembers();
                assertFalse(raftMembers.isEmpty());
            }
        });

        CPMember endpoint3 = instances[3].getCPSubsystem().getLocalCPMember();
        CPMember endpoint4 = instances[4].getCPSubsystem().getLocalCPMember();
        instances[3].getLifecycleService().terminate();
        instances[4].getLifecycleService().terminate();

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        CPGroupId groupId3 = invocationManager.createRaftGroup("groupId3", 3).get();
        CPGroupId groupId4 = invocationManager.createRaftGroup("g4", 4).get();

        RaftNodeImpl leaderNode = waitAllForLeaderElection(Arrays.copyOf(instances, 3), getMetadataGroupId(instances[0]));
        HazelcastInstance leader = getInstance(leaderNode.getLocalMember());
        CPGroup group3 = queryRaftGroupLocally(leader, groupId3);
        assertNotNull(group3);
        assertThat(group3.members(), not(hasItem(endpoint3)));
        assertThat(group3.members(), not(hasItem(endpoint4)));

        CPGroup group4 = queryRaftGroupLocally(leader, groupId4);
        assertNotNull(group4);
        boolean b3 = group4.members().contains(endpoint3);
        boolean b4 = group4.members().contains(endpoint4);
        assertTrue(b3 ^ b4);
    }

    @Test
    public void when_noCpNodeCountConfigured_then_cpDiscoveryCompletes() {
        HazelcastInstance[] instances = newInstances(0, 0, 3);

        for (HazelcastInstance instance : instances) {
            assertTrue(getRaftService(instance).isDiscoveryCompleted());
        }
    }

    @Test
    public void when_membersLeaveDuringInitialDiscovery_thenAllMembersTerminate() {
        int nodeCount = 5;
        int startedNodeCount = nodeCount - 1;
        Config config = createConfig(nodeCount, nodeCount);

        HazelcastInstance[] instances = new HazelcastInstance[startedNodeCount];
        Node[] nodes = new Node[startedNodeCount];
        for (int i = 0; i < startedNodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
            nodes[i] = getNode(instances[i]);
        }

        // wait for the cp discovery process to start
        sleepAtLeastSeconds(10);

        instances[0].getLifecycleService().terminate();

        assertTrueEventually(() -> {
            for (int i = 0; i < startedNodeCount; i++) {
                assertEquals(NodeState.SHUT_DOWN, nodes[i].getState());
            }
        });
    }

    @Test
    public void when_membersLeaveDuringDiscoveryAfterCPSubsystemRestart_then_discoveryIsCancelled() throws ExecutionException, InterruptedException {
        int nodeCount = 5;
        Config config = createConfig(nodeCount, nodeCount);

        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }

        waitUntilCPDiscoveryCompleted(instances);

        instances[2].getLifecycleService().terminate();
        instances[3].getLifecycleService().terminate();
        instances[4].getLifecycleService().terminate();

        instances[2] = factory.newHazelcastInstance(config);
        instances[3] = factory.newHazelcastInstance(config);

        // we are triggering the restart mechanism while there is a missing member already.
        // the cp subsystem discovery won't be able to complete and we kill another member meanwhile
        long seed = System.currentTimeMillis();
        for (HazelcastInstance instance : Arrays.copyOf(instances, nodeCount - 1)) {
            Address address = instance.getCluster().getLocalMember().getAddress();
            getNodeEngineImpl(instance).getOperationService()
                     .invokeOnTarget(RaftService.SERVICE_NAME, new ResetCPMemberOp(seed), address).get();
        }

        // wait for the cp discovery process to start
        sleepAtLeastSeconds(10);

        instances[3].getLifecycleService().terminate();

        assertTrueEventually(() -> {
            for (int i = 0; i < 3; i++) {
                assertTrue(getRaftService(instances[i]).getMetadataGroupManager().isDiscoveryCompleted());
                assertNull(instances[i].getCPSubsystem().getLocalCPMember());
            }
        });
    }

    @Test
    public void when_cpSubsystemReset_then_cpGroupIsCreatedWithDifferentGroupId() {
        int cpMemberCount = 3;
        HazelcastInstance[] instances = newInstances(cpMemberCount);
        waitUntilCPDiscoveryCompleted(instances);

        RaftGroupId groupId1 = getRaftInvocationManager(instances[0]).createRaftGroup(DEFAULT_GROUP_NAME).joinInternal();

        instances[1].getLifecycleService().terminate();
        instances[2].getLifecycleService().terminate();

        HazelcastInstance[] newInstances = new HazelcastInstance[cpMemberCount];
        newInstances[0] = instances[0];
        newInstances[1] = factory.newHazelcastInstance(createConfig(cpMemberCount, cpMemberCount));
        newInstances[2] = factory.newHazelcastInstance(createConfig(cpMemberCount, cpMemberCount));

        instances[0].getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().join();

        waitUntilCPDiscoveryCompleted(newInstances);

        RaftGroupId groupId2 = getRaftInvocationManager(newInstances[0]).createRaftGroup(DEFAULT_GROUP_NAME).joinInternal();

        assertNotEquals(groupId1.getId(), groupId2.getId());
    }

    @Test
    public void when_cpSubsystemStartsMultipleTimes_then_cpGroupIsCreatedWithDifferentGroupId() {
        int cpMemberCount = 3;
        HazelcastInstance[] instances = newInstances(cpMemberCount);
        waitUntilCPDiscoveryCompleted(instances);

        RaftGroupId groupId1 = getRaftInvocationManager(instances[0]).createRaftGroup(DEFAULT_GROUP_NAME).joinInternal();

        instances[0].getLifecycleService().terminate();
        instances[1].getLifecycleService().terminate();
        instances[2].getLifecycleService().terminate();

        HazelcastInstance[] newInstances = new HazelcastInstance[cpMemberCount];
        newInstances[0] = factory.newHazelcastInstance(createConfig(cpMemberCount, cpMemberCount));
        newInstances[1] = factory.newHazelcastInstance(createConfig(cpMemberCount, cpMemberCount));
        newInstances[2] = factory.newHazelcastInstance(createConfig(cpMemberCount, cpMemberCount));

        waitUntilCPDiscoveryCompleted(newInstances);

        RaftGroupId groupId2 = getRaftInvocationManager(newInstances[0]).createRaftGroup(DEFAULT_GROUP_NAME).joinInternal();

        assertNotEquals(groupId1, groupId2);
    }

    private CPGroupId createNewRaftGroup(HazelcastInstance instance, String name, int groupSize) {
        RaftInvocationManager invocationManager = getRaftService(instance).getInvocationManager();
        try {
            return invocationManager.createRaftGroup(name, groupSize).get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void destroyRaftGroup(HazelcastInstance instance, CPGroupId groupId) {
        RaftService service = getRaftService(instance);
        RaftInvocationManager invocationManager = service.getInvocationManager();
        try {
            invocationManager.invoke(service.getMetadataGroupId(), new TriggerDestroyRaftGroupOp(groupId)).get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private CPMember findCommonEndpoint(HazelcastInstance instance, CPGroupId groupId1, CPGroupId groupId2)
            throws ExecutionException, InterruptedException {
        CPGroup group1 = instance.getCPSubsystem().getCPSubsystemManagementService().getCPGroup(groupId1.getName())
                                 .toCompletableFuture().get();
        CPGroup group2 = instance.getCPSubsystem().getCPSubsystemManagementService().getCPGroup(groupId2.getName())
                                 .toCompletableFuture().get();

        Set<CPMember> members = new HashSet<>(group1.members());
        members.retainAll(group2.members());

        return members.isEmpty() ? null : members.iterator().next();
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.getCPSubsystemConfig().getRaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(1000);
        return config;
    }

}
