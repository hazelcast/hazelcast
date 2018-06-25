/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.command.ApplyRaftGroupMembersCmd;
import com.hazelcast.raft.impl.service.operation.metadata.GetActiveRaftMembersOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetMembershipChangeContextOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetRaftGroupOp;
import com.hazelcast.raft.impl.util.DummyOp;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.raft.impl.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.raft.impl.service.MetadataRaftGroupManager.METADATA_GROUP_ID;
import static com.hazelcast.util.FutureUtil.returnWithDeadline;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftMemberAddRemoveTest extends HazelcastRaftTestSupport {

    @Test
    public void testPromoteToRaftMember() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(2, 2, 1);

        final RaftService service = getRaftService(instances[instances.length - 1]);
        service.triggerRaftMemberPromotion().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(service.getMetadataGroupManager().getLocalMember());
            }
        });
    }

    @Test
    public void testRemoveRaftMember() throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(3);

        final RaftGroupId testGroupId = getRaftInvocationManager(instances[0]).createRaftGroup("test", 3).get();

        final RaftService service = getRaftService(instances[1]);
        Member member = instances[0].getCluster().getLocalMember();
        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(2, instances[1]);

        final RaftMemberImpl removedEndpoint = new RaftMemberImpl(member);
        service.triggerRemoveRaftMember(removedEndpoint).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftGroupInfo metadataGroupInfo = getRaftGroupLocally(instances[1], METADATA_GROUP_ID);
                assertEquals(2, metadataGroupInfo.memberCount());
                assertFalse(metadataGroupInfo.containsMember(removedEndpoint));

                RaftGroupInfo testGroupInfo = getRaftGroupLocally(instances[1], testGroupId);
                assertEquals(2, testGroupInfo.memberCount());
                assertFalse(testGroupInfo.containsMember(removedEndpoint));
            }
        });
    }

    @Test
    public void testRemoveRaftMemberIdempotency() throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(3);

        final RaftGroupId testGroupId = getRaftInvocationManager(instances[0]).createRaftGroup("test", 3).get();

        final RaftService service = getRaftService(instances[1]);
        Member member = instances[0].getCluster().getLocalMember();
        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(2, instances[1]);

        final RaftMemberImpl removedEndpoint = new RaftMemberImpl(member);
        service.triggerRemoveRaftMember(removedEndpoint).get();
        service.triggerRemoveRaftMember(removedEndpoint).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftGroupInfo metadataGroup = getRaftGroupLocally(instances[1], METADATA_GROUP_ID);
                assertEquals(2, metadataGroup.memberCount());
                assertFalse(metadataGroup.containsMember(removedEndpoint));

                RaftGroupInfo testGroup = getRaftGroupLocally(instances[1], testGroupId);
                assertEquals(2, testGroup.memberCount());
                assertFalse(testGroup.containsMember(removedEndpoint));
            }
        });
    }

    @Test
    public void testRemoveMemberFromForceDestroyedRaftGroup() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 0);

        waitAllForLeaderElection(instances, METADATA_GROUP_ID);

        RaftGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup("test", 2).get();
        RaftGroupInfo group = getRaftInvocationManager(instances[0]).<RaftGroupInfo>invoke(METADATA_GROUP_ID, new GetRaftGroupOp(groupId)).get();
        final RaftMemberImpl crashedMember = group.membersArray()[0];

        HazelcastInstance runningInstance = (getAddress(instances[0])).equals(crashedMember.getAddress()) ? instances[1] : instances[0];

        factory.getInstance(crashedMember.getAddress()).getLifecycleService().terminate();

        RaftService raftService = getRaftService(runningInstance);
        raftService.forceDestroyRaftGroup(groupId).get();
        raftService.triggerRemoveRaftMember(crashedMember).get();

        final RaftInvocationManager invocationManager = getRaftInvocationManager(runningInstance);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                List<RaftMember> activeMembers = invocationManager.<List<RaftMember>>query(METADATA_GROUP_ID, new GetActiveRaftMembersOp(), LEADER_LOCAL).get();
                assertFalse(activeMembers.contains(crashedMember));
            }
        });
    }

    @Test
    public void testRemoveMemberFromMajorityLostRaftGroup() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 0);

        waitAllForLeaderElection(instances, METADATA_GROUP_ID);

        RaftGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup("test", 2).get();

        getRaftInvocationManager(instances[0]).invoke(groupId, new DummyOp()).get();

        final RaftNodeImpl groupLeaderRaftNode = getLeaderNode(instances, groupId);
        RaftGroupInfo group = getRaftInvocationManager(instances[0]).<RaftGroupInfo>invoke(METADATA_GROUP_ID, new GetRaftGroupOp(groupId)).get();
        RaftMemberImpl[] groupMembers = group.membersArray();
        final RaftMemberImpl crashedMember = groupMembers[0].equals(groupLeaderRaftNode.getLocalMember()) ? groupMembers[1] : groupMembers[0];

        HazelcastInstance runningInstance = (getAddress(instances[0])).equals(crashedMember.getAddress()) ? instances[1] : instances[0];

        final RaftInvocationManager invocationManager = getRaftInvocationManager(runningInstance);

        factory.getInstance(crashedMember.getAddress()).getLifecycleService().terminate();

        // from now on, "test" group lost the majority

        RaftService raftService = getRaftService(runningInstance);

        // we triggered removal of the crashed member but we won't be able to commit to the "test" group
        raftService.triggerRemoveRaftMember(crashedMember).get();

        // wait until RaftCleanupHandler kicks in and appends ApplyRaftGroupMembersCmd to the leader of the "test" group
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getLastLogOrSnapshotEntry(groupLeaderRaftNode).operation() instanceof ApplyRaftGroupMembersCmd);
            }
        });

        // force-destroy the raft group.
        // Now, the pending membership change in the "test" group will fail and we will fix it in the metadata group.
        raftService.forceDestroyRaftGroup(groupId).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                MembershipChangeContext ctx = invocationManager.<MembershipChangeContext>query(METADATA_GROUP_ID, new GetMembershipChangeContextOp(), LEADER_LOCAL).get();
                assertNull(ctx);
            }
        });
    }

    @Test
    public void testRaftMemberNotPresentInAnyRaftGroupIsRemovedDirectlyAfterCrash() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        HazelcastInstance master = instances[0];
        final HazelcastInstance promoted = instances[instances.length - 1];
        getRaftService(promoted).triggerRaftMemberPromotion().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(getRaftService(promoted).getLocalMember());
            }
        });

        RaftMemberImpl promotedRaftMember = getRaftService(promoted).getLocalMember();
        promoted.getLifecycleService().terminate();

        getRaftService(master).triggerRemoveRaftMember(promotedRaftMember).get();

        MembershipChangeContext ctx = getRaftInvocationManager(master).<MembershipChangeContext>query(METADATA_GROUP_ID, new GetMembershipChangeContextOp(), LEADER_LOCAL).get();
        assertNull(ctx);
    }

    @Test
    public void testRaftMemberNotPresentInAnyRaftGroupIsRemovedDirectlyForGracefulShutdown() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        HazelcastInstance master = instances[0];
        final HazelcastInstance promoted = instances[instances.length - 1];
        getRaftService(promoted).triggerRaftMemberPromotion().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(getRaftService(promoted).getLocalMember());
            }
        });

        promoted.getLifecycleService().shutdown();

        MembershipChangeContext ctx = getRaftInvocationManager(master).<MembershipChangeContext>query(METADATA_GROUP_ID, new GetMembershipChangeContextOp(), LEADER_LOCAL).get();
        assertNull(ctx);
    }

    @Test
    public void testMetadataGroupReinitializationAfterLostMajority() {
        HazelcastInstance[] instances = newInstances(3, 3, 1);
        waitUntilCPDiscoveryCompleted(instances);

        instances[1].getLifecycleService().terminate();
        instances[2].getLifecycleService().terminate();
        assertClusterSizeEventually(2, instances[3]);

        final HazelcastInstance[] newInstances = new HazelcastInstance[3];
        newInstances[0] = instances[0];
        newInstances[1] = instances[3];

        getRaftService(newInstances[0]).resetAndInitRaftState();
        getRaftService(newInstances[1]).resetAndInitRaftState();

        Config config = createConfig(3, 3);
        newInstances[2] = factory.newHazelcastInstance(config);

        waitAllForLeaderElection(newInstances, METADATA_GROUP_ID);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftGroupInfo group = getRaftGroupLocally(newInstances[2], METADATA_GROUP_ID);
                assertNotNull(group);
                Collection<RaftMemberImpl> endpoints = group.memberImpls();

                for (HazelcastInstance instance : newInstances) {
                    Member localMember = instance.getCluster().getLocalMember();
                    RaftMemberImpl endpoint = new RaftMemberImpl(localMember);
                    assertTrue(endpoints.contains(endpoint));
                }
            }
        });
    }

    @Test
    public void testRaftInvocationsAfterMetadataGroupReinitialization() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);
        waitUntilCPDiscoveryCompleted(instances);

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

        for (HazelcastInstance hz : instances) {
            getRaftService(hz).resetAndInitRaftState();
        }

        List<RaftMember> newEndpoints = getRaftInvocationManager(instance).<List<RaftMember>>invoke(METADATA_GROUP_ID, new GetActiveRaftMembersOp()).get();
        assertEquals(3, newEndpoints.size());
    }

    @Test
    public void testResetRaftStateWhileMajorityIsReachable() throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(3);
        waitUntilCPDiscoveryCompleted(instances);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[2]);

        instances[0].getLifecycleService().terminate();
        assertClusterSizeEventually(2, instances[1], instances[2]);

        getRaftService(instances[1]).resetAndInitRaftState();
        getRaftService(instances[2]).resetAndInitRaftState();

        Config config = createConfig(3, 3);
        instances[0] = factory.newHazelcastInstance(config);

        List<RaftMember> newEndpoints = invocationManager.<List<RaftMember>>invoke(METADATA_GROUP_ID, new GetActiveRaftMembersOp()).get();
        for (HazelcastInstance instance : instances) {
            assertTrue(newEndpoints.contains(new RaftMemberImpl(instance.getCluster().getLocalMember())));
        }
    }

    @Test
    public void testStartNewAPMember_afterDiscoveryIsCompleted() throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(3);

        waitUntilCPDiscoveryCompleted(instances);

        instances[2].getLifecycleService().terminate();
        assertClusterSizeEventually(2, instances[1]);

        Config config = createConfig(3, 3);
        instances[2] = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, instances[1]);
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertTrue(instances[2].getLifecycleService().isRunning());
            }
        }, 5);
    }

    @Test
    public void testExpandRaftGroup() throws ExecutionException, InterruptedException, TimeoutException {
        final HazelcastInstance[] instances = newInstances(3, 3, 1);

        final RaftInvocationManager invocationManager = getRaftInvocationManager(instances[3]);

        instances[0].shutdown();

        getRaftService(instances[3]).triggerRaftMemberPromotion().get(30, TimeUnit.SECONDS);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws ExecutionException, InterruptedException {
                RaftGroupInfo group = invocationManager.<RaftGroupInfo>query(METADATA_GROUP_ID, new GetRaftGroupOp(METADATA_GROUP_ID), LEADER_LOCAL).get();
                assertEquals(3, group.memberCount());
                Collection<RaftMember> members = group.members();
                assertTrue(members.contains(getRaftService(instances[3]).getLocalMember()));

                assertNotNull(getRaftNode(instances[3], METADATA_GROUP_ID));
            }
        });
    }

    @Test
    public void testExpandRaftGroupMultipleTimes() throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(5, 5, 3);

        final RaftInvocationManager invocationManager = getRaftInvocationManager(instances[3]);

        instances[0].shutdown();
        instances[1].shutdown();
        instances[2].shutdown();

        getRaftService(instances[5]).triggerRaftMemberPromotion().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws ExecutionException, InterruptedException {
                RaftGroupInfo group = invocationManager.<RaftGroupInfo>query(METADATA_GROUP_ID, new GetRaftGroupOp(METADATA_GROUP_ID), LEADER_LOCAL).get();
                assertEquals(3, group.memberCount());
                Collection<RaftMember> members = group.members();
                assertTrue(members.contains(getRaftService(instances[5]).getLocalMember()));
                assertNotNull(getRaftNode(instances[5], METADATA_GROUP_ID));
            }
        });

        getRaftService(instances[6]).triggerRaftMemberPromotion().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws ExecutionException, InterruptedException {
                RaftGroupInfo group = invocationManager.<RaftGroupInfo>query(METADATA_GROUP_ID, new GetRaftGroupOp(METADATA_GROUP_ID), LEADER_LOCAL).get();
                assertEquals(4, group.memberCount());
                Collection<RaftMember> members = group.members();
                assertTrue(members.contains(getRaftService(instances[6]).getLocalMember()));
                assertNotNull(getRaftNode(instances[6], METADATA_GROUP_ID));
            }
        });

        getRaftService(instances[7]).triggerRaftMemberPromotion().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws ExecutionException, InterruptedException {
                RaftGroupInfo group = invocationManager.<RaftGroupInfo>query(METADATA_GROUP_ID, new GetRaftGroupOp(METADATA_GROUP_ID), LEADER_LOCAL).get();
                assertEquals(5, group.memberCount());
                Collection<RaftMember> members = group.members();
                assertTrue(members.contains(getRaftService(instances[7]).getLocalMember()));

                assertNotNull(getRaftNode(instances[7], METADATA_GROUP_ID));
            }
        });
    }

    @Test
    public void testExpandMultipleRaftGroupsMultipleTimes() throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(5, 5, 2);

        final RaftInvocationManager invocationManager = getRaftInvocationManager(instances[6]);
        final RaftGroupId groupId = invocationManager.createRaftGroup("g1", 5).get();
        invocationManager.invoke(groupId, new DummyOp()).get();

        RaftGroupInfo otherGroup = invocationManager.<RaftGroupInfo>invoke(METADATA_GROUP_ID, new GetRaftGroupOp(groupId)).get();
        RaftMemberImpl[] otherGroupMembers = otherGroup.membersArray();
        List<Address> shutdownAddresses = Arrays.asList(otherGroupMembers[0].getAddress(), otherGroupMembers[1].getAddress());

        for (Address address : shutdownAddresses) {
            factory.getInstance(address).shutdown();
        }

        getRaftService(instances[5]).triggerRaftMemberPromotion().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws ExecutionException, InterruptedException {
                RaftGroupInfo metadataGroup = invocationManager.<RaftGroupInfo>query(METADATA_GROUP_ID, new GetRaftGroupOp(METADATA_GROUP_ID), LEADER_LOCAL).get();
                RaftGroupInfo otherGroup = invocationManager.<RaftGroupInfo>query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId), LEADER_LOCAL).get();
                assertEquals(4, metadataGroup.memberCount());
                assertEquals(4, otherGroup.memberCount());

                assertNotNull(getRaftNode(instances[5], METADATA_GROUP_ID));
                assertNotNull(getRaftNode(instances[5], groupId));
            }
        });

        getRaftService(instances[6]).triggerRaftMemberPromotion().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws ExecutionException, InterruptedException {
                RaftGroupInfo metadataGroup = invocationManager.<RaftGroupInfo>query(METADATA_GROUP_ID, new GetRaftGroupOp(METADATA_GROUP_ID), LEADER_LOCAL).get();
                RaftGroupInfo otherGroup = invocationManager.<RaftGroupInfo>query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId), LEADER_LOCAL).get();
                assertEquals(5, metadataGroup.memberCount());
                assertEquals(5, otherGroup.memberCount());

                assertNotNull(getRaftNode(instances[6], METADATA_GROUP_ID));
                assertNotNull(getRaftNode(instances[5], groupId));
            }
        });
    }

    @Test
    public void testNodeBecomesAP_whenInitialRaftMemberCount_isBiggerThanConfiguredNumber() {
        int cpNodeCount = 3;
        HazelcastInstance[] instances = newInstances(cpNodeCount);

        Config config = createConfig(cpNodeCount, cpNodeCount);
        final HazelcastInstance instance = factory.newHazelcastInstance(config);

        waitAllForLeaderElection(instances, METADATA_GROUP_ID);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNull(getRaftService(instance).getLocalMember());
            }
        });
    }

    @Test
    public void testNodeTerminates_whenInitialRaftMembers_doesNotMatch() {
        HazelcastInstance[] instances = newInstances(3);

        instances[2].getLifecycleService().terminate();

        try {
            Config config = createConfig(3, 3);
            final HazelcastInstance instance = factory.newHazelcastInstance(config);
            getRaftService(instance).resetAndInitRaftState();

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertFalse(instance.getLifecycleService().isRunning());
                }
            });
        } catch (IllegalStateException ignored) {
        }

        waitAllForLeaderElection(Arrays.copyOf(instances, 2), METADATA_GROUP_ID);
    }

    @Test
    public void testNodesBecomeAP_whenMoreThanInitialRaftMembers_areStartedConcurrently() {
        final Config config = createConfig(3, 2);

        final Collection<Future<HazelcastInstance>> futures = new ArrayList<Future<HazelcastInstance>>();
        int nodeCount = 8;
        for (int i = 0; i < nodeCount; i++) {
            Future<HazelcastInstance> future = spawn(new Callable<HazelcastInstance>() {
                @Override
                public HazelcastInstance call() {
                    return factory.newHazelcastInstance(config);
                }
            });
            futures.add(future);
        }

        final Collection<HazelcastInstance> instances =
                returnWithDeadline(futures, ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        assertClusterSizeEventually(nodeCount, instances);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int cpCount = 0;
                int metadataCount = 0;
                for (HazelcastInstance instance : instances) {
                    assertTrue(instance.getLifecycleService().isRunning());
                    if (getRaftService(instance).getLocalMember() != null) {
                        cpCount++;
                    }
                    if (getRaftGroupLocally(instance, METADATA_GROUP_ID) != null) {
                        metadataCount++;
                    }
                }
                assertEquals(3, cpCount);
                assertEquals(2, metadataCount);
            }
        });
    }

    @Test
    public void testNodesTerminate_whenInitialRaftMembersConflict() {
        // TODO
    }

}
