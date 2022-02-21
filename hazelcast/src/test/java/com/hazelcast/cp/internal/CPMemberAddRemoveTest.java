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
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.command.UpdateRaftGroupMembersCmd;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.GetMembershipChangeScheduleOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.instance.StaticMemberNodeContext;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.instance.impl.HazelcastInstanceFactory.newHazelcastInstance;
import static com.hazelcast.internal.util.FutureUtil.returnWithDeadline;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.TestHazelcastInstanceFactory.initOrCreateConfig;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class CPMemberAddRemoveTest extends HazelcastRaftTestSupport {

    @Test
    public void testAwaitDiscoveryCompleted() throws InterruptedException {
        Config config = createConfig(3, 3);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        assertFalse(hz1.getCPSubsystem().getCPSubsystemManagementService().awaitUntilDiscoveryCompleted(1, TimeUnit.SECONDS));
        assertFalse(hz2.getCPSubsystem().getCPSubsystemManagementService().awaitUntilDiscoveryCompleted(1, TimeUnit.SECONDS));

        HazelcastInstance hz3 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(3, hz1, hz2, hz3);

        assertTrue(hz1.getCPSubsystem().getCPSubsystemManagementService().awaitUntilDiscoveryCompleted(60, TimeUnit.SECONDS));
        assertTrue(hz2.getCPSubsystem().getCPSubsystemManagementService().awaitUntilDiscoveryCompleted(60, TimeUnit.SECONDS));
        assertTrue(hz3.getCPSubsystem().getCPSubsystemManagementService().awaitUntilDiscoveryCompleted(60, TimeUnit.SECONDS));

        HazelcastInstance hz4 = factory.newHazelcastInstance(config);
        assertTrue(hz4.getCPSubsystem().getCPSubsystemManagementService().isDiscoveryCompleted());
    }

    @Test
    public void testPromoteToRaftMember() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        HazelcastInstance instance = instances[instances.length - 1];
        instance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                .toCompletableFuture().get();

        assertNotNull(instance.getCPSubsystem().getLocalCPMember());
    }

    @Test
    public void testRemoveRaftMember() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3);

        getRaftInvocationManager(instances[0]).createRaftGroup("test", 3).get();

        Member member = instances[0].getCluster().getLocalMember();
        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(2, instances[1]);

        CPMemberInfo removedEndpoint = new CPMemberInfo(member);
        instances[1].getCPSubsystem().getCPSubsystemManagementService().removeCPMember(removedEndpoint.getUuid())
                    .toCompletableFuture().get();

        CPGroup metadataGroup = instances[1].getCPSubsystem()
                                            .getCPSubsystemManagementService()
                                            .getCPGroup(METADATA_CP_GROUP_NAME)
                                            .toCompletableFuture()
                                            .get();
        assertEquals(2, metadataGroup.members().size());
        assertFalse(metadataGroup.members().contains(removedEndpoint));

        CPGroup testGroup = instances[1].getCPSubsystem()
                                        .getCPSubsystemManagementService()
                                        .getCPGroup("test")
                                        .toCompletableFuture()
                                        .get();
        assertNotNull(testGroup);
        assertEquals(2, testGroup.members().size());
        assertFalse(testGroup.members().contains(removedEndpoint));
    }

    @Test
    public void testRemoveMemberFromForceDestroyedRaftGroup() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 0);

        waitAllForLeaderElection(instances, getMetadataGroupId(instances[0]));

        CPGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup("test", 2).get();
        CPGroup group = instances[0].getCPSubsystem().getCPSubsystemManagementService().getCPGroup(groupId.getName())
                                    .toCompletableFuture().get();

        CPMember crashedMember = group.members().iterator().next();

        HazelcastInstance runningInstance = (getAddress(instances[0])).equals(crashedMember.getAddress()) ? instances[1] : instances[0];

        factory.getInstance(crashedMember.getAddress()).getLifecycleService().terminate();

        CPSubsystemManagementService cpSubsystemManagementService = runningInstance.getCPSubsystem()
                                                                                   .getCPSubsystemManagementService();
        cpSubsystemManagementService.forceDestroyCPGroup(groupId.getName())
                                    .toCompletableFuture().get();
        cpSubsystemManagementService.removeCPMember(crashedMember.getUuid())
                                    .toCompletableFuture().get();

        Collection<CPMember> activeMembers = cpSubsystemManagementService.getCPMembers()
                                                                         .toCompletableFuture().get();
        assertFalse(activeMembers.contains(crashedMember));
    }

    @Test
    public void testRemoveMemberFromMajorityLostRaftGroup() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 0);

        waitAllForLeaderElection(instances, getMetadataGroupId(instances[0]));

        CPGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup("test", 2).get();

        getRaftInvocationManager(instances[0]).invoke(groupId, new DummyOp()).get();

        RaftNodeImpl groupLeaderRaftNode = getLeaderNode(instances, groupId);
        CPGroup group = instances[0].getCPSubsystem().getCPSubsystemManagementService().getCPGroup(groupId.getName())
                                    .toCompletableFuture().get();

        CPMember[] groupMembers = group.members().toArray(new CPMember[0]);
        CPMember crashedMember = groupMembers[0].getUuid().equals(groupLeaderRaftNode.getLocalMember().getUuid())
                ? groupMembers[1] : groupMembers[0];

        HazelcastInstance runningInstance = (getAddress(instances[0])).equals(crashedMember.getAddress()) ? instances[1] : instances[0];

        RaftInvocationManager invocationManager = getRaftInvocationManager(runningInstance);

        factory.getInstance(crashedMember.getAddress()).getLifecycleService().terminate();

        // from now on, "test" group lost the majority

        // we triggered removal of the crashed member but we won't be able to commit to the "test" group
        CompletableFuture<Void> f = runningInstance.getCPSubsystem()
                                                   .getCPSubsystemManagementService()
                                                   .removeCPMember(crashedMember.getUuid())
                                                   .toCompletableFuture();

        // wait until RaftCleanupHandler kicks in and appends ApplyRaftGroupMembersCmd to the leader of the "test" group
        assertTrueEventually(
                () -> assertTrue(getLastLogOrSnapshotEntry(groupLeaderRaftNode).operation() instanceof UpdateRaftGroupMembersCmd));

        // force-destroy the raft group.
        // Now, the pending membership change in the "test" group will fail and we will fix it in the metadata group.
        runningInstance.getCPSubsystem().getCPSubsystemManagementService().forceDestroyCPGroup(groupId.getName())
                       .toCompletableFuture().get();

        f.get();

        MembershipChangeSchedule schedule = invocationManager.<MembershipChangeSchedule>query(getMetadataGroupId(runningInstance),
                new GetMembershipChangeScheduleOp(), LINEARIZABLE).get();
        assertNull(schedule);
    }

    @Test
    public void testRaftMemberNotPresentInAnyRaftGroupIsRemovedDirectlyAfterCrash() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        HazelcastInstance master = instances[0];
        HazelcastInstance promoted = instances[instances.length - 1];
        promoted.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                .toCompletableFuture().get();

        CPMember promotedMember = promoted.getCPSubsystem().getLocalCPMember();
        promoted.getLifecycleService().terminate();

        master.getCPSubsystem().getCPSubsystemManagementService().removeCPMember(promotedMember.getUuid())
              .toCompletableFuture().get();

        MembershipChangeSchedule schedule = getRaftInvocationManager(master).<MembershipChangeSchedule>query(getMetadataGroupId(master),
                new GetMembershipChangeScheduleOp(), LINEARIZABLE).get();
        assertNull(schedule);
    }

    @Test
    public void testRaftMemberIsRemovedForGracefulShutdown() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 0);

        CPMember shutdownCPMember = instances[0].getCPSubsystem().getLocalCPMember();

        instances[0].getLifecycleService().shutdown();

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[1]);
        CPGroupId metadataGroupId = getMetadataGroupId(instances[1]);
        MembershipChangeSchedule schedule = invocationManager.<MembershipChangeSchedule>query(metadataGroupId,
                new GetMembershipChangeScheduleOp(), LINEARIZABLE).get();
        assertNull(schedule);
        CPGroup group = invocationManager.<CPGroup>invoke(metadataGroupId, new GetRaftGroupOp(metadataGroupId)).join();
        assertEquals(2, group.members().size());
        for (CPMember member : group.members()) {
            assertNotEquals(shutdownCPMember, member);
        }
    }

    @Test
    public void testRaftMemberNotPresentInAnyRaftGroupIsRemovedDirectlyForGracefulShutdown() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        HazelcastInstance master = instances[0];
        HazelcastInstance promoted = instances[instances.length - 1];
        promoted.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                .toCompletableFuture().get();

        promoted.getLifecycleService().shutdown();

        MembershipChangeSchedule schedule = getRaftInvocationManager(master).<MembershipChangeSchedule>query(getMetadataGroupId(master),
                new GetMembershipChangeScheduleOp(), LINEARIZABLE).get();
        assertNull(schedule);
    }

    @Test
    public void testMetadataGroupReinitializationAfterLostMajority() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        long groupIdSeed = getRaftService(instances[0]).getMetadataGroupManager().getGroupIdSeed();
        RaftGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup(CPGroup.DEFAULT_GROUP_NAME).get();

        IAtomicLong long1 = instances[0].getCPSubsystem().getAtomicLong("proxy");

        sleepAtLeastMillis(10);

        instances[1].getLifecycleService().terminate();
        instances[2].getLifecycleService().terminate();
        assertClusterSizeEventually(2, instances[3]);

        HazelcastInstance[] newInstances = new HazelcastInstance[3];
        newInstances[0] = instances[0];
        newInstances[1] = instances[3];

        Config config = createConfig(3, 3);
        newInstances[2] = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, newInstances);
        newInstances[0].getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().get();
        waitUntilCPDiscoveryCompleted(newInstances);

        long newGroupIdSeed = getRaftService(newInstances[0]).getMetadataGroupManager().getGroupIdSeed();
        RaftGroupId newGroupId = getRaftInvocationManager(instances[0]).createRaftGroup(CPGroup.DEFAULT_GROUP_NAME).get();

        assertThat(newGroupIdSeed, greaterThan(groupIdSeed));
        assertThat(newGroupId.getSeed(), greaterThan(groupId.getSeed()));

        try {
            long1.incrementAndGet();
            fail();
        } catch (CPGroupDestroyedException ignored) {
        }

        IAtomicLong long2 = newInstances[2].getCPSubsystem().getAtomicLong("proxy");
        long2.incrementAndGet();

        assertTrueEventually(() -> {
            CPGroupSummary group = queryRaftGroupLocally(newInstances[2], getMetadataGroupId(newInstances[2]));
            assertNotNull(group);
            Collection<CPMember> endpoints = group.members();

            for (HazelcastInstance instance : newInstances) {
                Member localMember = instance.getCluster().getLocalMember();
                assertThat(new CPMemberInfo(localMember), isIn(endpoints));
            }
        });
    }

    @Test
    public void testRaftInvocationsAfterMetadataGroupReinitialization() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        HazelcastInstance instance = instances[3];

        instances[0].getLifecycleService().terminate();
        instances[1].getLifecycleService().terminate();
        instances[2].getLifecycleService().terminate();
        assertClusterSizeEventually(1, instance);

        instances = new HazelcastInstance[3];
        instances[0] = instance;

        Config config = createConfig(3, 3);
        instances[1] = factory.newHazelcastInstance(config);
        instances[2] = factory.newHazelcastInstance(config);

        instance.getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().get();

        List<CPMemberInfo> newEndpoints = getRaftInvocationManager(instance).<List<CPMemberInfo>>query(getMetadataGroupId(instance),
                new GetActiveCPMembersOp(), LINEARIZABLE).get();
        assertEquals(3, newEndpoints.size());
    }

    @Test
    public void testResetRaftStateWhileMajorityIsReachable() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[2]);

        instances[0].getLifecycleService().terminate();
        assertClusterSizeEventually(2, instances[1], instances[2]);

        Config config = createConfig(3, 3);
        instances[0] = factory.newHazelcastInstance(config);

        instances[1].getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().get();

        List<CPMemberInfo> newEndpoints = invocationManager.<List<CPMemberInfo>>query(getMetadataGroupId(instances[2]),
                new GetActiveCPMembersOp(), LINEARIZABLE).get();
        for (HazelcastInstance instance : instances) {
            assertTrue(newEndpoints.contains(new CPMemberInfo(instance.getCluster().getLocalMember())));
        }
    }

    @Test
    public void testStartNewAPMember_afterDiscoveryIsCompleted() {
        HazelcastInstance[] instances = newInstances(3);

        instances[2].getLifecycleService().terminate();
        assertClusterSizeEventually(2, instances[1]);

        Config config = createConfig(3, 3);
        instances[2] = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, instances[1]);
        assertTrueAllTheTime(() -> assertTrue(instances[2].getLifecycleService().isRunning()), 5);
    }

    @Test
    public void testExpandRaftGroup() throws ExecutionException, InterruptedException, TimeoutException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        instances[0].shutdown();

        instances[3].getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);

        CPGroupId metadataGroupId = getMetadataGroupId(instances[1]);
        CPGroup group = instances[1].getCPSubsystem().getCPSubsystemManagementService().getCPGroup(METADATA_CP_GROUP_NAME)
                                    .toCompletableFuture().get();
        assertEquals(3, group.members().size());
        Collection<CPMember> members = group.members();
        assertTrue(members.contains(instances[3].getCPSubsystem().getLocalCPMember()));

        assertTrueEventually(() -> assertNotNull(getRaftNode(instances[3], metadataGroupId)));
    }

    @Test
    public void testExpandRaftGroupMultipleTimes() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(5, 5, 3);

        CPGroupId metadataGroupId = getMetadataGroupId(instances[0]);

        waitAllForLeaderElection(Arrays.copyOf(instances, 5), metadataGroupId);

        instances[0].shutdown();
        instances[1].shutdown();
        instances[2].shutdown();

        CPSubsystemManagementService managementService = instances[3].getCPSubsystem().getCPSubsystemManagementService();
        CPGroup group = managementService.getCPGroup(metadataGroupId.getName())
                                         .toCompletableFuture().get();
        assertEquals(2, group.members().size());

        instances[5].getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                    .toCompletableFuture().get();

        group = managementService.getCPGroup(metadataGroupId.getName())
                                 .toCompletableFuture().get();
        assertEquals(3, group.members().size());
        Collection<CPMember> members = group.members();
        assertTrue(members.contains(instances[5].getCPSubsystem().getLocalCPMember()));
        assertTrueEventually(() -> assertNotNull(getRaftNode(instances[5], metadataGroupId)));

        instances[6].getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                    .toCompletableFuture().get();

        group = managementService.getCPGroup(metadataGroupId.getName())
                                 .toCompletableFuture().get();
        assertEquals(4, group.members().size());
        members = group.members();
        assertTrue(members.contains(instances[6].getCPSubsystem().getLocalCPMember()));
        assertTrueEventually(() -> assertNotNull(getRaftNode(instances[5], metadataGroupId)));

        instances[7].getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                    .toCompletableFuture().get();

        group = managementService.getCPGroup(metadataGroupId.getName())
                                 .toCompletableFuture().get();
        assertEquals(5, group.members().size());
        members = group.members();
        assertTrue(members.contains(instances[7].getCPSubsystem().getLocalCPMember()));
        assertTrueEventually(() -> assertNotNull(getRaftNode(instances[5], metadataGroupId)));

        CPGroup metadataGroup = group;

        HazelcastInstance[] newInstances = {instances[3], instances[4], instances[5], instances[6], instances[7]};

        assertTrueEventually(() -> {
            long commitIndex = getCommitIndex(getLeaderNode(newInstances, metadataGroupId));

            for (int i = 3; i < instances.length; i++) {
                HazelcastInstance instance = instances[i];
                RaftNodeImpl raftNode = getRaftNode(instance, metadataGroupId);
                assertNotNull(raftNode);
                assertEquals(commitIndex, getCommitIndex(raftNode));
                CPGroup g = queryRaftGroupLocally(instance, metadataGroupId);
                assertNotNull(g);
                assertArrayEquals(metadataGroup.members().toArray(new CPMember[0]), g.members().toArray(new CPMember[0]));
            }
        });
    }

    @Test
    public void testExpandMultipleRaftGroupsMultipleTimes() throws ExecutionException, InterruptedException, TimeoutException {
        HazelcastInstance[] instances = newInstances(5, 5, 2);

        CPGroupId metadataGroupId = getMetadataGroupId(instances[0]);

        CPSubsystemManagementService managementService = instances[6].getCPSubsystem().getCPSubsystemManagementService();
        String groupName = "group1";
        instances[0].getCPSubsystem().getAtomicLong("long1@" + groupName).set(5);
        CPGroup otherGroup = managementService.getCPGroup(groupName)
                                              .toCompletableFuture().get();
        CPGroupId groupId = otherGroup.id();

        waitAllForLeaderElection(Arrays.copyOf(instances, 5), groupId);

        CPMember[] otherGroupMembers = otherGroup.members().toArray(new CPMember[0]);
        List<Address> shutdownAddresses = asList(otherGroupMembers[0].getAddress(), otherGroupMembers[1].getAddress());

        instances[5].getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);

        HazelcastInstance[] runningInstances = new HazelcastInstance[instances.length - shutdownAddresses.size()];
        for (int i = 0, j = 0; i < instances.length; i++) {
            HazelcastInstance instance = instances[i];
            if (shutdownAddresses.contains(getAddress(instance))) {
                instance.shutdown();
            } else {
                runningInstances[j++] = instance;
            }
        }

        CPGroup metadataGroup = managementService.getCPGroup(metadataGroupId.getName())
                                                 .toCompletableFuture().get();
        otherGroup = managementService.getCPGroup(groupName).toCompletableFuture().get();
        assertEquals(4, metadataGroup.members().size());
        assertEquals(4, otherGroup.members().size());

        assertTrueEventually(() -> {
            assertNotNull(getRaftNode(instances[5], metadataGroupId));
            assertNotNull(getRaftNode(instances[5], groupId));
        });

        instances[6].getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);

        metadataGroup = managementService.getCPGroup(metadataGroupId.getName())
                                         .toCompletableFuture().get();
        otherGroup = managementService.getCPGroup(groupName).toCompletableFuture().get();
        assertEquals(5, metadataGroup.members().size());
        assertEquals(5, otherGroup.members().size());

        CPGroup metadataGroupRef = metadataGroup;
        CPGroup otherGroupRef = otherGroup;

        assertTrueEventually(() -> {
            long commitIndex = getCommitIndex(getLeaderNode(runningInstances, metadataGroupId));

            assertNotNull(getRaftNode(instances[6], groupId));

            for (HazelcastInstance instance : asList(instances[5], instances[6])) {
                RaftNodeImpl raftNode = getRaftNode(instance, metadataGroupId);
                assertNotNull(raftNode);
                assertEquals(commitIndex, getCommitIndex(raftNode));

                CPGroup g1 = queryRaftGroupLocally(instance, metadataGroupId);
                CPGroup g2 = queryRaftGroupLocally(instance, otherGroupRef.id());
                assertNotNull(g1);
                assertNotNull(g2);

                assertArrayEquals(metadataGroupRef.members().toArray(new CPMember[0]), g1.members().toArray(new CPMember[0]));
                assertArrayEquals(otherGroupRef.members().toArray(new CPMember[0]), g2.members().toArray(new CPMember[0]));
            }
        });
    }

    @Test
    public void testNodeBecomesAP_whenInitialRaftMemberCount_isBiggerThanConfiguredNumber() {
        int cpNodeCount = 3;
        HazelcastInstance[] instances = newInstances(cpNodeCount);

        Config config = createConfig(cpNodeCount, cpNodeCount);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        waitAllForLeaderElection(instances, getMetadataGroupId(instances[0]));

        assertTrueEventually(() -> assertNull(instance.getCPSubsystem().getLocalCPMember()));
    }

    @Test
    public void test_sessionClosedOnCPSubsystemReset() throws Exception {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        instances[0].getCPSubsystem().getAtomicLong("long1").set(1);
        instances[0].getCPSubsystem().getAtomicLong("long1@custom").set(2);

        FencedLock lock = instances[3].getCPSubsystem().getLock("lock");
        lock.lock();

        instances[0].getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().get();

        assertTrueEventually(() -> {
            ProxySessionManagerService service = getNodeEngineImpl(instances[3]).getService(ProxySessionManagerService.SERVICE_NAME);
            assertEquals(NO_SESSION_ID, service.getSession((RaftGroupId) lock.getGroupId()));
        });
    }

    @Test
    public void testNodesBecomeAP_whenMoreThanInitialRaftMembers_areStartedConcurrently() {
        Config config = createConfig(4, 3);

        Collection<Future<HazelcastInstance>> futures = new ArrayList<>();
        int nodeCount = 8;
        for (int i = 0; i < nodeCount; i++) {
            Future<HazelcastInstance> future = spawn(() -> factory.newHazelcastInstance(config));
            futures.add(future);
        }

        Collection<HazelcastInstance> instances = returnWithDeadline(futures, ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertClusterSizeEventually(nodeCount, instances);
        HazelcastInstance[] instancesArray = instances.toArray(new HazelcastInstance[0]);
        waitUntilCPDiscoveryCompleted(instancesArray);

        assertTrueEventually(() -> {
            int cpCount = 0;
            int metadataCount = 0;
            long commitIndex = getCommitIndex(getLeaderNode(instancesArray, getMetadataGroupId(instancesArray[0])));

            for (HazelcastInstance instance : instances) {
                assertTrue(instance.getLifecycleService().isRunning());
                if (instance.getCPSubsystem().getLocalCPMember() != null) {
                    cpCount++;
                }

                RaftNodeImpl raftNode = getRaftNode(instance, getMetadataGroupId(instance));
                if (raftNode != null) {
                    assertEquals(commitIndex, getCommitIndex(raftNode));

                    if (queryRaftGroupLocally(instance, getMetadataGroupId(instance)) != null) {
                        metadataCount++;
                    }
                }
            }
            assertEquals(4, cpCount);
            assertEquals(3, metadataCount);
        });
    }

    @Test
    public void testCPMemberIdentityChanges_whenLocalMemberIsRecovered_duringRestart() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3);
        waitAllForLeaderElection(instances, getMetadataGroupId(instances[0]));

        Member localMember = instances[0].getCluster().getLocalMember();
        CPMember localCpMember = instances[0].getCPSubsystem().getLocalCPMember();
        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(2, instances[1]);

        instances[1].getCPSubsystem().getCPSubsystemManagementService().removeCPMember(localCpMember.getUuid())
                    .toCompletableFuture().get();

        instances[0] = newHazelcastInstance(initOrCreateConfig(createConfig(3, 3)), randomString(),
                new StaticMemberNodeContext(factory, localMember));
        assertEquals(localMember, instances[0].getCluster().getLocalMember());

        assertTrueAllTheTime(() -> assertNull(instances[0].getCPSubsystem().getLocalCPMember()), 5);

        instances[0].getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                    .toCompletableFuture().get();
        assertNotNull(instances[0].getCPSubsystem().getLocalCPMember());
        assertNotEquals(localCpMember, instances[0].getCPSubsystem().getLocalCPMember());
    }

    @Test
    public void when_newCPMemberIsAddedToTheMetadataGroupAfterSnapshot_newMemberInstallsSnapshot() throws ExecutionException, InterruptedException {
        int nodeCount = 3;
        int commitIndexAdvanceCountToSnapshot = 50;
        Config config = createConfig(nodeCount, nodeCount);
        config.getCPSubsystemConfig()
              .getRaftAlgorithmConfig()
              .setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCountToSnapshot);

        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }

        assertClusterSizeEventually(nodeCount, instances);
        waitUntilCPDiscoveryCompleted(instances);

        instances[0].getCPSubsystem().getAtomicLong("long@group1").set(1);
        instances[0].getCPSubsystem().getAtomicLong("long@group2").set(2);

        for (int i = 0; i < commitIndexAdvanceCountToSnapshot; i++) {
            getRaftInvocationManager(instances[0]).invoke(getMetadataGroupId(instances[0]), new GetActiveCPMembersOp()).get();
        }

        assertTrueEventually(
                () -> assertTrue(getSnapshotEntry(getLeaderNode(instances, getMetadataGroupId(instances[0]))).index() >= commitIndexAdvanceCountToSnapshot));

        for (int i = 0; i < 5; i++) {
            instances[0].getCPSubsystem().getCPSubsystemManagementService().getCPGroup(METADATA_CP_GROUP_NAME)
                        .toCompletableFuture().get();
        }

        instances[0].shutdown();

        HazelcastInstance newInstance = factory.newHazelcastInstance(config);
        newInstance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                   .toCompletableFuture().get();

        CPGroup metadataGroup = newInstance.getCPSubsystem()
                                           .getCPSubsystemManagementService()
                                           .getCPGroup(METADATA_CP_GROUP_NAME)
                                           .toCompletableFuture()
                                           .get();

        CPGroup group1 = newInstance.getCPSubsystem()
                                    .getCPSubsystemManagementService()
                                    .getCPGroup("group1")
                                    .toCompletableFuture()
                                    .get();

        CPGroup group2 = newInstance.getCPSubsystem()
                                    .getCPSubsystemManagementService()
                                    .getCPGroup("group2")
                                    .toCompletableFuture()
                                    .get();

        List<CPMember> cpMembers = new ArrayList<>(newInstance.getCPSubsystem().getCPSubsystemManagementService().getCPMembers()
                                                              .toCompletableFuture().get());

        assertTrueEventually(() -> {
            long commitIndex = getCommitIndex(getLeaderNode(new HazelcastInstance[]{instances[1], instances[2], newInstance},
                    metadataGroup.id()));

            RaftNodeImpl raftNode = getRaftNode(newInstance, metadataGroup.id());
            assertNotNull(raftNode);
            assertEquals(commitIndex, getCommitIndex(raftNode));

            CPGroup m = queryRaftGroupLocally(newInstance, metadataGroup.id());
            CPGroup g1 = queryRaftGroupLocally(newInstance, group1.id());
            CPGroup g2 = queryRaftGroupLocally(newInstance, group2.id());

            assertNotNull(m);
            assertNotNull(g1);
            assertNotNull(g2);

            assertArrayEquals(metadataGroup.members().toArray(new CPMember[0]), m.members().toArray(new CPMember[0]));
            assertArrayEquals(group1.members().toArray(new CPMember[0]), g1.members().toArray(new CPMember[0]));
            assertArrayEquals(group2.members().toArray(new CPMember[0]), g2.members().toArray(new CPMember[0]));

            List<CPMemberInfo> activeMembers = new ArrayList<>(getRaftService(newInstance).getMetadataGroupManager().getActiveMembers());
            assertEquals(cpMembers, activeMembers);
        });
    }

    @Test
    public void when_snapshotIsTakenWhileRemovingCPMember_newMemberInstallsSnapshot() throws Exception {
        int nodeCount = 3;
        int commitIndexAdvanceCountToSnapshot = 50;
        Config config = createConfig(nodeCount, nodeCount);
        config.getCPSubsystemConfig().getRaftAlgorithmConfig()
              .setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCountToSnapshot);

        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }

        assertClusterSizeEventually(nodeCount, instances);
        waitUntilCPDiscoveryCompleted(instances);

        // `commitIndexAdvanceCountToSnapshot - 5` is selected on purpose to partially include removal of CP member in snapshot.
        // Specifically, RemoveCPMemberOp will be in snapshot but CompleteRaftGroupMembershipChangesOp will not.
        for (int i = 0; i < commitIndexAdvanceCountToSnapshot - 5; i++) {
            getRaftInvocationManager(instances[0]).invoke(getMetadataGroupId(instances[0]), new GetActiveCPMembersOp()).get();
        }

        // This will add 3 entries, RemoveCPMemberOp, ChangeRaftGroupMembersCmd and CompleteRaftGroupMembershipChangesOp.
        // RemoveCPMemberOp will be in snapshot but CompleteRaftGroupMembershipChangesOp will not be included.
        instances[0].shutdown();

        HazelcastInstance newInstance = factory.newHazelcastInstance(config);
        newInstance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember().toCompletableFuture().join();

        List<CPMember> cpMembers = new ArrayList<>(newInstance.getCPSubsystem()
                                                              .getCPSubsystemManagementService()
                                                              .getCPMembers()
                                                              .toCompletableFuture().join());

        assertTrueEventually(() -> {
            RaftService service = getRaftService(newInstance);
            List<CPMemberInfo> activeMembers = new ArrayList<>(service.getMetadataGroupManager().getActiveMembers());
            assertEquals(cpMembers, activeMembers);
        });
    }

    @Test
    public void when_snapshotIsTakenWhileRemovingCPLeader_newMemberInstallsSnapshot() throws Exception {
        int nodeCount = 3;
        int commitIndexAdvanceCountToSnapshot = 50;
        Config config = createConfig(nodeCount, nodeCount);
        config.getCPSubsystemConfig().getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCountToSnapshot);

        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }

        assertClusterSizeEventually(nodeCount, instances);
        waitUntilCPDiscoveryCompleted(instances);

        HazelcastInstance leaderInstance = getLeaderInstance(instances, getMetadataGroupId(instances[0]));

        // `commitIndexAdvanceCountToSnapshot - 5` is selected on purpose to partially include removal of CP member in snapshot.
        // Specifically, RemoveCPMemberOp will be in snapshot but CompleteRaftGroupMembershipChangesOp will not.
        for (int i = 0; i < commitIndexAdvanceCountToSnapshot - 5; i++) {
            getRaftInvocationManager(leaderInstance).invoke(getMetadataGroupId(instances[0]), new GetActiveCPMembersOp()).get();
        }

        // This will add 3 entries, RemoveCPMemberOp, ChangeRaftGroupMembersCmd and CompleteRaftGroupMembershipChangesOp.
        // RemoveCPMemberOp will be in snapshot but CompleteRaftGroupMembershipChangesOp will not be included.
        leaderInstance.shutdown();

        HazelcastInstance newInstance = factory.newHazelcastInstance(config);
        newInstance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember().toCompletableFuture().join();

        List<CPMember> cpMembers = new ArrayList<>(newInstance.getCPSubsystem().getCPSubsystemManagementService().getCPMembers().toCompletableFuture().join());

        assertTrueEventually(() -> {
            RaftService service = getRaftService(newInstance);
            List<CPMemberInfo> activeMembers = new ArrayList<>(service.getMetadataGroupManager().getActiveMembers());
            assertEquals(cpMembers, activeMembers);
        });
    }


    @Test
    public void when_newCPMemberIsAddedToTheMetadataGroupAfterRestart_newMemberCommitsMetadataGroupLogEntries() throws ExecutionException, InterruptedException {
        int nodeCount = 3;
        Config config = createConfig(nodeCount, nodeCount);

        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }

        RaftGroupId initialMetadataGroupId = getMetadataGroupId(instances[0]);

        assertClusterSizeEventually(nodeCount, instances);
        waitUntilCPDiscoveryCompleted(instances);

        instances[0].getCPSubsystem().getCPSubsystemManagementService().getCPGroup(METADATA_CP_GROUP_NAME)
                    .toCompletableFuture().get();

        instances[1].getLifecycleService().terminate();
        instances[2].getLifecycleService().terminate();

        HazelcastInstance newInstance1 = factory.newHazelcastInstance(config);
        HazelcastInstance newInstance2 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, instances[0], newInstance1, newInstance2);

        instances[0].getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().get();

        RaftGroupId newMetadataGroupId = getRaftService(instances[0]).getMetadataGroupId();
        assertTrue(newMetadataGroupId.getSeed() > initialMetadataGroupId.getSeed());
        assertEquals(newMetadataGroupId.getSeed(), getRaftService(newInstance1).getMetadataGroupId().getSeed());
        assertEquals(newMetadataGroupId.getSeed(), getRaftService(newInstance2).getMetadataGroupId().getSeed());

        instances[0].getCPSubsystem().getAtomicLong("long@group1").set(1);
        instances[0].getCPSubsystem().getAtomicLong("long@group2").set(2);

        instances[0].shutdown();

        HazelcastInstance newInstance3 = factory.newHazelcastInstance(config);
        newInstance3.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                    .toCompletableFuture().get();

        CPGroup metadataGroup = newInstance1.getCPSubsystem()
                                            .getCPSubsystemManagementService()
                                            .getCPGroup(METADATA_CP_GROUP_NAME)
                                            .toCompletableFuture()
                                            .get();

        CPGroup group1 = newInstance1.getCPSubsystem()
                                     .getCPSubsystemManagementService()
                                     .getCPGroup("group1")
                                     .toCompletableFuture()
                                     .get();

        CPGroup group2 = newInstance1.getCPSubsystem()
                                     .getCPSubsystemManagementService()
                                     .getCPGroup("group2")
                                     .toCompletableFuture()
                                     .get();

        List<CPMember> cpMembers =
                new ArrayList<>(newInstance1.getCPSubsystem().getCPSubsystemManagementService().getCPMembers()
                                            .toCompletableFuture().get());

        HazelcastInstance[] newInstances = new HazelcastInstance[]{newInstance1, newInstance2, newInstance3};

        assertTrueEventually(() -> {
            long commitIndex = getCommitIndex(getLeaderNode(newInstances, getMetadataGroupId(newInstance1)));
            for (HazelcastInstance instance : Arrays.asList(newInstance1, newInstance2, newInstance3)) {
                assertEquals(newMetadataGroupId.getSeed(), getMetadataGroupId(instance).getSeed());

                RaftNodeImpl raftNode = getRaftNode(instance, getMetadataGroupId(instance));
                assertNotNull(raftNode);
                assertEquals(commitIndex, getCommitIndex(raftNode));

                CPGroup m = queryRaftGroupLocally(instance, metadataGroup.id());
                CPGroup g1 = queryRaftGroupLocally(instance, group1.id());
                CPGroup g2 = queryRaftGroupLocally(instance, group2.id());

                assertNotNull(m);
                assertNotNull(g1);
                assertNotNull(g2);

                assertArrayEquals(metadataGroup.members().toArray(new CPMember[0]), m.members().toArray(new CPMember[0]));
                assertArrayEquals(group1.members().toArray(new CPMember[0]), g1.members().toArray(new CPMember[0]));
                assertArrayEquals(group2.members().toArray(new CPMember[0]), g2.members().toArray(new CPMember[0]));

                List<CPMemberInfo> activeMembers = new ArrayList<>(getRaftService(instance).getMetadataGroupManager().getActiveMembers());
                assertEquals(cpMembers, activeMembers);
            }
        });
    }

    @Test
    public void when_newCPMemberIsAddedToTheMetadataGroupAfterRestartAndSnapshot_newMemberInstallsSnapshot() throws ExecutionException, InterruptedException {
        int nodeCount = 3;
        int commitIndexAdvanceCountToSnapshot = 50;
        Config config = createConfig(nodeCount, nodeCount);
        config.getCPSubsystemConfig()
              .getRaftAlgorithmConfig()
              .setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCountToSnapshot);

        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }

        assertClusterSizeEventually(nodeCount, instances);
        waitUntilCPDiscoveryCompleted(instances);

        RaftGroupId initialMetadataGroupId = getMetadataGroupId(instances[0]);

        instances[0].getCPSubsystem().getCPSubsystemManagementService().getCPGroup(METADATA_CP_GROUP_NAME)
                    .toCompletableFuture().get();

        instances[1].getLifecycleService().terminate();
        instances[2].getLifecycleService().terminate();

        HazelcastInstance newInstance1 = factory.newHazelcastInstance(config);
        HazelcastInstance newInstance2 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, instances[0], newInstance1, newInstance2);

        instances[0].getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().get();

        RaftGroupId newMetadataGroupId = getRaftService(instances[0]).getMetadataGroupId();
        assertTrue(newMetadataGroupId.getSeed() > initialMetadataGroupId.getSeed());
        assertEquals(newMetadataGroupId.getSeed(), getRaftService(newInstance1).getMetadataGroupId().getSeed());
        assertEquals(newMetadataGroupId.getSeed(), getRaftService(newInstance2).getMetadataGroupId().getSeed());

        for (int i = 0; i < commitIndexAdvanceCountToSnapshot; i++) {
            getRaftInvocationManager(instances[0]).invoke(getMetadataGroupId(instances[0]), new GetActiveCPMembersOp()).get();
        }

        assertTrueEventually(() -> {
            assertTrue(getSnapshotEntry(getRaftNode(instances[0], newMetadataGroupId)).index() >= commitIndexAdvanceCountToSnapshot);
            assertTrue(getSnapshotEntry(getRaftNode(newInstance1, newMetadataGroupId)).index() >= commitIndexAdvanceCountToSnapshot);
            assertTrue(getSnapshotEntry(getRaftNode(newInstance2, newMetadataGroupId)).index() >= commitIndexAdvanceCountToSnapshot);
        });

        instances[0].getCPSubsystem().getAtomicLong("long@group1").set(1);
        instances[0].getCPSubsystem().getAtomicLong("long@group2").set(2);

        instances[0].shutdown();

        HazelcastInstance newInstance3 = factory.newHazelcastInstance(config);
        newInstance3.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                    .toCompletableFuture().get();

        CPGroup metadataGroup = newInstance1.getCPSubsystem()
                                            .getCPSubsystemManagementService()
                                            .getCPGroup(METADATA_CP_GROUP_NAME)
                                            .toCompletableFuture()
                                            .get();

        CPGroup group1 = newInstance1.getCPSubsystem()
                                     .getCPSubsystemManagementService()
                                     .getCPGroup("group1")
                                     .toCompletableFuture()
                                     .get();

        CPGroup group2 = newInstance1.getCPSubsystem()
                                     .getCPSubsystemManagementService()
                                     .getCPGroup("group2")
                                     .toCompletableFuture()
                                     .get();

        List<CPMember> cpMembers =
                new ArrayList<>(newInstance1.getCPSubsystem().getCPSubsystemManagementService().getCPMembers()
                                            .toCompletableFuture().get());

        HazelcastInstance[] newInstances = new HazelcastInstance[]{newInstance1, newInstance2, newInstance3};

        assertTrueEventually(() -> {
            long commitIndex = getCommitIndex(getLeaderNode(newInstances, newMetadataGroupId));
            for (HazelcastInstance instance : Arrays.asList(newInstance1, newInstance2, newInstance3)) {
                assertEquals(newMetadataGroupId.getSeed(), getMetadataGroupId(instance).getSeed());

                RaftNodeImpl raftNode = getRaftNode(instance, newMetadataGroupId);
                assertNotNull(raftNode);
                assertEquals(commitIndex, getCommitIndex(raftNode));

                CPGroup m = queryRaftGroupLocally(instance, metadataGroup.id());
                CPGroup g1 = queryRaftGroupLocally(instance, group1.id());
                CPGroup g2 = queryRaftGroupLocally(instance, group2.id());

                assertNotNull(m);
                assertNotNull(g1);
                assertNotNull(g2);

                assertArrayEquals(metadataGroup.members().toArray(new CPMember[0]), m.members().toArray(new CPMember[0]));
                assertArrayEquals(group1.members().toArray(new CPMember[0]), g1.members().toArray(new CPMember[0]));
                assertArrayEquals(group2.members().toArray(new CPMember[0]), g2.members().toArray(new CPMember[0]));

                List<CPMemberInfo> activeMembers = new ArrayList<>(getRaftService(instance).getMetadataGroupManager().getActiveMembers());
                assertEquals(cpMembers, activeMembers);
            }
        });
    }

    @Test
    public void when_cpMembersShutdownConcurrently_then_theyCompleteTheirShutdown() throws ExecutionException, InterruptedException {
        // When there are N CP members, we can perform partially-concurrent shutdown in 2 steps:
        // In the first step, we shut down N - 2 members concurrently.
        // Once those members are done, we shutdown the last 2 CP members serially.
        // The last 2 CP members must be shutdown serially because if both of them shutdown at the same time,
        // one of them can commit its leave to the Metadata group and terminate before the other one performs its commit.
        // In this case, the last member hangs since there is no available majority of the Metadata group anymore.

        HazelcastInstance[] instances = newInstances(7, 5, 0);

        int concurrent = 5;
        Future[] futures = new Future[concurrent];
        for (int i = 0; i < concurrent; i++) {
            int ix = i;
            futures[i] = spawn(() -> instances[ix].shutdown());
        }

        for (Future f : futures) {
            assertCompletesEventually(f);
            f.get();
        }

        for (int i = 0, remaining = (instances.length - concurrent); i < remaining; i++) {
            instances[concurrent + i].shutdown();
        }
    }

    @Test
    public void when_cpMembersShutdownSequentially_then_theyCompleteTheirShutdown() {
        HazelcastInstance[] instances = newInstances(5, 3, 2);

        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }

    @Test
    public void when_clusterIsShutdown_then_allCPMembersCompleteShutdown() {
        HazelcastInstance[] instances = newInstances(5, 3, 1);

        Node[] nodes = new Node[instances.length];
        for (int i = 0; i < instances.length; i++) {
            nodes[i] = getNode(instances[i]);
        }

        assertClusterSizeEventually(instances.length, instances);
        waitAllForSafeState(instances);

        instances[0].getCluster().shutdown();

        assertTrueEventually(() -> {
            for (int i = 0; i < instances.length; i++) {
                assertEquals(NodeState.SHUT_DOWN, nodes[i].getState());
            }
        });
    }

    @Test
    public void when_crashedMemberIsReplacedByAnotherAvailableCPMember_then_membershipChangeSucceeds() throws InterruptedException, ExecutionException {
        int cpMemberCount = 3;
        HazelcastInstance[] instances = newInstances(cpMemberCount);
        waitUntilCPDiscoveryCompleted(instances);

        HazelcastInstance instance4 = factory.newHazelcastInstance(createConfig(cpMemberCount, cpMemberCount));
        instance4.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                 .toCompletableFuture().get();

        CPMember cpMember3 = instances[2].getCPSubsystem().getLocalCPMember();
        instances[2].getLifecycleService().terminate();
        instances[0].getCPSubsystem().getCPSubsystemManagementService().removeCPMember(cpMember3.getUuid());

        assertTrueEventually(() -> {
            CPGroup metadataGroup = instances[0].getCPSubsystem()
                                                .getCPSubsystemManagementService()
                                                .getCPGroup(CPGroup.METADATA_CP_GROUP_NAME)
                                                .toCompletableFuture()
                                                .get();
            assertTrue(metadataGroup.members().contains(instance4.getCPSubsystem().getLocalCPMember()));
            assertEquals(cpMemberCount, metadataGroup.members().size());
        });
    }

    @Test
    public void when_crashedMemberIsRemovedAndThenNewCPMemberIsPromoted_then_membershipChangeSucceeds()
            throws ExecutionException, InterruptedException {
        int cpMemberCount = 3;
        HazelcastInstance[] instances = newInstances(cpMemberCount);
        waitUntilCPDiscoveryCompleted(instances);

        CPMember cpMember3 = instances[2].getCPSubsystem().getLocalCPMember();
        instances[2].getLifecycleService().terminate();
        instances[0].getCPSubsystem().getCPSubsystemManagementService().removeCPMember(cpMember3.getUuid());

        assertTrueEventually(() -> {
            CPGroup metadataGroup = instances[0].getCPSubsystem()
                                                .getCPSubsystemManagementService()
                                                .getCPGroup(CPGroup.METADATA_CP_GROUP_NAME)
                                                .toCompletableFuture()
                                                .get();
            assertEquals(cpMemberCount - 1, metadataGroup.members().size());
            assertFalse(metadataGroup.members().contains(cpMember3));
        });

        HazelcastInstance instance4 = factory.newHazelcastInstance(createConfig(cpMemberCount, cpMemberCount));
        instance4.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                 .toCompletableFuture().get();

        assertTrueEventually(() -> {
            CPGroup metadataGroup = instances[0].getCPSubsystem()
                                                .getCPSubsystemManagementService()
                                                .getCPGroup(CPGroup.METADATA_CP_GROUP_NAME)
                                                .toCompletableFuture()
                                                .get();
            assertTrue(metadataGroup.members().contains(instance4.getCPSubsystem().getLocalCPMember()));
            assertEquals(cpMemberCount, metadataGroup.members().size());
        });
    }

}
