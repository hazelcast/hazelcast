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

package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.RaftAtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.RaftCountDownLatchService;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.command.ApplyRaftGroupMembersCmd;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.GetMembershipChangeContextOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.instance.StaticMemberNodeContext;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.proxyservice.InternalProxyService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
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

import static com.hazelcast.cp.internal.MetadataRaftGroupManager.INITIAL_METADATA_GROUP_ID;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.instance.HazelcastInstanceFactory.newHazelcastInstance;
import static com.hazelcast.test.TestHazelcastInstanceFactory.initOrCreateConfig;
import static com.hazelcast.util.FutureUtil.returnWithDeadline;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CPMemberAddRemoveTest extends HazelcastRaftTestSupport {

    @Test
    public void testPromoteToRaftMember() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        final RaftService service = getRaftService(instances[instances.length - 1]);
        service.promoteToCPMember().get();

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

        final CPGroupId testGroupId = getRaftInvocationManager(instances[0]).createRaftGroup("test", 3).get();

        Member member = instances[0].getCluster().getLocalMember();
        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(2, instances[1]);

        final CPMemberInfo removedEndpoint = new CPMemberInfo(member);
        instances[1].getCPSubsystem().getCPSubsystemManagementService().removeCPMember(removedEndpoint.getUuid()).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                CPGroupInfo metadataGroupInfo = getRaftGroupLocally(instances[1], getMetadataGroupId(instances[1]));
                assertEquals(2, metadataGroupInfo.memberCount());
                assertFalse(metadataGroupInfo.containsMember(removedEndpoint));

                CPGroupInfo testGroupInfo = getRaftGroupLocally(instances[1], testGroupId);
                assertEquals(2, testGroupInfo.memberCount());
                assertFalse(testGroupInfo.containsMember(removedEndpoint));
            }
        });
    }

    @Test
    public void testRemoveRaftMemberIdempotency() throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(3);

        final CPGroupId testGroupId = getRaftInvocationManager(instances[0]).createRaftGroup("test", 3).get();

        Member member = instances[0].getCluster().getLocalMember();
        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(2, instances[1]);

        final CPMemberInfo removedEndpoint = new CPMemberInfo(member);
        instances[1].getCPSubsystem().getCPSubsystemManagementService().removeCPMember(removedEndpoint.getUuid()).get();
        instances[1].getCPSubsystem().getCPSubsystemManagementService().removeCPMember(removedEndpoint.getUuid()).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                CPGroupInfo metadataGroup = getRaftGroupLocally(instances[1], getMetadataGroupId(instances[1]));
                assertEquals(2, metadataGroup.memberCount());
                assertFalse(metadataGroup.containsMember(removedEndpoint));

                CPGroupInfo testGroup = getRaftGroupLocally(instances[1], testGroupId);
                assertEquals(2, testGroup.memberCount());
                assertFalse(testGroup.containsMember(removedEndpoint));
            }
        });
    }

    @Test
    public void testRemoveMemberFromForceDestroyedRaftGroup() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 0);

        waitAllForLeaderElection(instances, INITIAL_METADATA_GROUP_ID);

        CPGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup("test", 2).get();
        CPGroupInfo group = getRaftInvocationManager(instances[0]).<CPGroupInfo>invoke(getMetadataGroupId(instances[0]),
                new GetRaftGroupOp(groupId)).get();
        final CPMemberInfo crashedMember = group.membersArray()[0];

        final HazelcastInstance runningInstance = (getAddress(instances[0])).equals(crashedMember.getAddress()) ? instances[1] : instances[0];

        factory.getInstance(crashedMember.getAddress()).getLifecycleService().terminate();

        runningInstance.getCPSubsystem().getCPSubsystemManagementService().forceDestroyCPGroup(groupId.name()).get();
        runningInstance.getCPSubsystem().getCPSubsystemManagementService().removeCPMember(crashedMember.getUuid()).get();

        final RaftInvocationManager invocationManager = getRaftInvocationManager(runningInstance);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                List<CPMemberInfo> activeMembers = invocationManager.<List<CPMemberInfo>>query(getMetadataGroupId(runningInstance),
                        new GetActiveCPMembersOp(), LEADER_LOCAL).get();
                assertFalse(activeMembers.contains(crashedMember));
            }
        });
    }

    @Test
    public void testRemoveMemberFromMajorityLostRaftGroup() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 0);

        waitAllForLeaderElection(instances, INITIAL_METADATA_GROUP_ID);

        CPGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup("test", 2).get();

        getRaftInvocationManager(instances[0]).invoke(groupId, new DummyOp()).get();

        final RaftNodeImpl groupLeaderRaftNode = getLeaderNode(instances, groupId);
        CPGroupInfo group = getRaftInvocationManager(instances[0]).<CPGroupInfo>invoke(getMetadataGroupId(instances[0]),
                new GetRaftGroupOp(groupId)).get();
        CPMemberInfo[] groupMembers = group.membersArray();
        final CPMemberInfo crashedMember = groupMembers[0].equals(groupLeaderRaftNode.getLocalMember()) ? groupMembers[1] : groupMembers[0];

        final HazelcastInstance runningInstance = (getAddress(instances[0])).equals(crashedMember.getAddress()) ? instances[1] : instances[0];

        final RaftInvocationManager invocationManager = getRaftInvocationManager(runningInstance);

        factory.getInstance(crashedMember.getAddress()).getLifecycleService().terminate();

        // from now on, "test" group lost the majority

        // we triggered removal of the crashed member but we won't be able to commit to the "test" group
        runningInstance.getCPSubsystem().getCPSubsystemManagementService().removeCPMember(crashedMember.getUuid()).get();

        // wait until RaftCleanupHandler kicks in and appends ApplyRaftGroupMembersCmd to the leader of the "test" group
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getLastLogOrSnapshotEntry(groupLeaderRaftNode).operation() instanceof ApplyRaftGroupMembersCmd);
            }
        });

        // force-destroy the raft group.
        // Now, the pending membership change in the "test" group will fail and we will fix it in the metadata group.
        runningInstance.getCPSubsystem().getCPSubsystemManagementService().forceDestroyCPGroup(groupId.name()).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                MembershipChangeContext ctx = invocationManager.<MembershipChangeContext>query(getMetadataGroupId(runningInstance),
                        new GetMembershipChangeContextOp(), LEADER_LOCAL).get();
                assertNull(ctx);
            }
        });
    }

    @Test
    public void testRaftMemberNotPresentInAnyRaftGroupIsRemovedDirectlyAfterCrash() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        HazelcastInstance master = instances[0];
        final HazelcastInstance promoted = instances[instances.length - 1];
        getRaftService(promoted).promoteToCPMember().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(getRaftService(promoted).getLocalMember());
            }
        });

        CPMemberInfo promotedMember = getRaftService(promoted).getLocalMember();
        promoted.getLifecycleService().terminate();

        master.getCPSubsystem().getCPSubsystemManagementService().removeCPMember(promotedMember.getUuid()).get();

        MembershipChangeContext ctx = getRaftInvocationManager(master).<MembershipChangeContext>query(getMetadataGroupId(master),
                new GetMembershipChangeContextOp(), LEADER_LOCAL).get();
        assertNull(ctx);
    }

    @Test
    public void testRaftMemberNotPresentInAnyRaftGroupIsRemovedDirectlyForGracefulShutdown() throws ExecutionException, InterruptedException {
        HazelcastInstance[] instances = newInstances(3, 3, 1);

        HazelcastInstance master = instances[0];
        final HazelcastInstance promoted = instances[instances.length - 1];
        getRaftService(promoted).promoteToCPMember().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(getRaftService(promoted).getLocalMember());
            }
        });

        promoted.getLifecycleService().shutdown();

        MembershipChangeContext ctx = getRaftInvocationManager(master).<MembershipChangeContext>query(getMetadataGroupId(master),
                new GetMembershipChangeContextOp(), LEADER_LOCAL).get();
        assertNull(ctx);
    }

    @Test
    public void testMetadataGroupReinitializationAfterLostMajority() throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(3, 3, 1);
        waitUntilCPDiscoveryCompleted(instances);

        long groupIdTerm = getRaftService(instances[0]).getMetadataGroupManager().getGroupIdSeed();
        RaftGroupId groupId = (RaftGroupId) getRaftInvocationManager(instances[0]).createRaftGroup(CPGroup.DEFAULT_GROUP_NAME).get();

        instances[0].getCPSubsystem().getAtomicLong("proxy");
        instances[0].getCPSubsystem().getAtomicReference("proxy");
        instances[0].getCPSubsystem().getLock("proxy");
        instances[0].getCPSubsystem().getSemaphore("proxy");
        instances[0].getCPSubsystem().getCountDownLatch("proxy");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    InternalProxyService proxyService = getNodeEngineImpl(instance).getProxyService();
                    assertFalse(proxyService.getDistributedObjectNames(RaftAtomicLongService.SERVICE_NAME).isEmpty());
                    assertFalse(proxyService.getDistributedObjectNames(RaftAtomicRefService.SERVICE_NAME).isEmpty());
                    assertFalse(proxyService.getDistributedObjectNames(RaftLockService.SERVICE_NAME).isEmpty());
                    assertFalse(proxyService.getDistributedObjectNames(RaftSemaphoreService.SERVICE_NAME).isEmpty());
                    assertFalse(proxyService.getDistributedObjectNames(RaftCountDownLatchService.SERVICE_NAME).isEmpty());
                }
            }
        });

        sleepAtLeastMillis(10);

        instances[1].getLifecycleService().terminate();
        instances[2].getLifecycleService().terminate();
        assertClusterSizeEventually(2, instances[3]);

        final HazelcastInstance[] newInstances = new HazelcastInstance[3];
        newInstances[0] = instances[0];
        newInstances[1] = instances[3];

        Config config = createConfig(3, 3);
        newInstances[2] = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, newInstances);
        newInstances[0].getCPSubsystem().getCPSubsystemManagementService().restart().get();
        waitUntilCPDiscoveryCompleted(newInstances);

        long newGroupIdTerm = getRaftService(newInstances[0]).getMetadataGroupManager().getGroupIdSeed();
        RaftGroupId newGroupId = (RaftGroupId) getRaftInvocationManager(instances[0]).createRaftGroup(CPGroup.DEFAULT_GROUP_NAME).get();

        assertThat(newGroupIdTerm, greaterThan(groupIdTerm));
        assertThat(newGroupId.seed(), greaterThan(groupId.seed()));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : newInstances) {
                    InternalProxyService proxyService = getNodeEngineImpl(instance).getProxyService();
                    Matcher<Collection<? extends String>> empty = Matchers.empty();
                    assertThat(proxyService.getDistributedObjectNames(RaftAtomicLongService.SERVICE_NAME), empty);
                    assertThat(proxyService.getDistributedObjectNames(RaftAtomicRefService.SERVICE_NAME), empty);
                    assertThat(proxyService.getDistributedObjectNames(RaftLockService.SERVICE_NAME), empty);
                    assertThat(proxyService.getDistributedObjectNames(RaftSemaphoreService.SERVICE_NAME), empty);
                    assertThat(proxyService.getDistributedObjectNames(RaftCountDownLatchService.SERVICE_NAME), empty);
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                CPGroupInfo group = getRaftGroupLocally(newInstances[2], getMetadataGroupId(newInstances[2]));
                assertNotNull(group);
                Collection<CPMemberInfo> endpoints = group.memberImpls();

                for (HazelcastInstance instance : newInstances) {
                    Member localMember = instance.getCluster().getLocalMember();
                    CPMemberInfo endpoint = new CPMemberInfo(localMember);
                    assertThat(endpoint, isIn(endpoints));
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

        instance.getCPSubsystem().getCPSubsystemManagementService().restart().get();

        List<CPMemberInfo> newEndpoints = getRaftInvocationManager(instance).<List<CPMemberInfo>>invoke(getMetadataGroupId(instance),
                new GetActiveCPMembersOp()).get();
        assertEquals(3, newEndpoints.size());
    }

    @Test
    public void testResetRaftStateWhileMajorityIsReachable() throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(3);
        waitUntilCPDiscoveryCompleted(instances);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[2]);

        instances[0].getLifecycleService().terminate();
        assertClusterSizeEventually(2, instances[1], instances[2]);

        Config config = createConfig(3, 3);
        instances[0] = factory.newHazelcastInstance(config);

        instances[1].getCPSubsystem().getCPSubsystemManagementService().restart().get();

        List<CPMemberInfo> newEndpoints = invocationManager.<List<CPMemberInfo>>invoke(getMetadataGroupId(instances[2]),
                new GetActiveCPMembersOp()).get();
        for (HazelcastInstance instance : instances) {
            assertTrue(newEndpoints.contains(new CPMemberInfo(instance.getCluster().getLocalMember())));
        }
    }

    @Test
    public void testStartNewAPMember_afterDiscoveryIsCompleted() {
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

        getRaftService(instances[3]).promoteToCPMember().get(30, TimeUnit.SECONDS);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws ExecutionException, InterruptedException {
                CPGroupId metadataGroupId = getMetadataGroupId(instances[3]);
                CPGroupInfo group = invocationManager.<CPGroupInfo>query(metadataGroupId, new GetRaftGroupOp(metadataGroupId), LEADER_LOCAL).get();
                assertEquals(3, group.memberCount());
                Collection<CPMemberInfo> members = group.memberImpls();
                assertTrue(members.contains(getRaftService(instances[3]).getLocalMember()));
                assertNotNull(getRaftNode(instances[3], metadataGroupId));
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

        getRaftService(instances[5]).promoteToCPMember().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws ExecutionException, InterruptedException {
                CPGroupId metadataGroupId = getMetadataGroupId(instances[3]);
                CPGroupInfo group = invocationManager.<CPGroupInfo>query(metadataGroupId, new GetRaftGroupOp(metadataGroupId), LEADER_LOCAL).get();
                assertEquals(3, group.memberCount());
                Collection<CPMemberInfo> members = group.memberImpls();
                assertTrue(members.contains(getRaftService(instances[5]).getLocalMember()));
                assertNotNull(getRaftNode(instances[5], metadataGroupId));
            }
        });

        getRaftService(instances[6]).promoteToCPMember().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws ExecutionException, InterruptedException {
                CPGroupId metadataGroupId = getMetadataGroupId(instances[3]);
                CPGroupInfo group = invocationManager.<CPGroupInfo>query(metadataGroupId, new GetRaftGroupOp(metadataGroupId), LEADER_LOCAL).get();
                assertEquals(4, group.memberCount());
                Collection<CPMemberInfo> members = group.memberImpls();
                assertTrue(members.contains(getRaftService(instances[6]).getLocalMember()));
                assertNotNull(getRaftNode(instances[6], metadataGroupId));
            }
        });

        getRaftService(instances[7]).promoteToCPMember().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws ExecutionException, InterruptedException {
                CPGroupId metadataGroupId = getMetadataGroupId(instances[3]);
                CPGroupInfo group = invocationManager.<CPGroupInfo>query(metadataGroupId, new GetRaftGroupOp(metadataGroupId), LEADER_LOCAL).get();
                assertEquals(5, group.memberCount());
                Collection<CPMemberInfo> members = group.memberImpls();
                assertTrue(members.contains(getRaftService(instances[7]).getLocalMember()));
                assertNotNull(getRaftNode(instances[7], metadataGroupId));
            }
        });
    }

    @Test
    public void testExpandMultipleRaftGroupsMultipleTimes() throws ExecutionException, InterruptedException {
        final HazelcastInstance[] instances = newInstances(5, 5, 2);

        final RaftInvocationManager invocationManager = getRaftInvocationManager(instances[6]);
        final CPGroupId groupId = invocationManager.createRaftGroup("g1", 5).get();
        invocationManager.invoke(groupId, new DummyOp()).get();

        CPGroupInfo otherGroup = invocationManager.<CPGroupInfo>invoke(getMetadataGroupId(instances[6]), new GetRaftGroupOp(groupId)).get();
        CPMemberInfo[] otherGroupMembers = otherGroup.membersArray();
        List<Address> shutdownAddresses = Arrays.asList(otherGroupMembers[0].getAddress(), otherGroupMembers[1].getAddress());

        for (Address address : shutdownAddresses) {
            factory.getInstance(address).shutdown();
        }

        getRaftService(instances[5]).promoteToCPMember().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws ExecutionException, InterruptedException {
                CPGroupId metadataGroupId = getMetadataGroupId(instances[6]);
                CPGroupInfo metadataGroup = invocationManager.<CPGroupInfo>query(metadataGroupId, new GetRaftGroupOp(metadataGroupId), LEADER_LOCAL).get();
                CPGroupInfo otherGroup = invocationManager.<CPGroupInfo>query(metadataGroupId, new GetRaftGroupOp(groupId), LEADER_LOCAL).get();
                assertEquals(4, metadataGroup.memberCount());
                assertEquals(4, otherGroup.memberCount());

                assertNotNull(getRaftNode(instances[5], metadataGroupId));
                assertNotNull(getRaftNode(instances[5], groupId));
            }
        });

        getRaftService(instances[6]).promoteToCPMember().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws ExecutionException, InterruptedException {
                CPGroupId metadataGroupId = getMetadataGroupId(instances[6]);
                CPGroupInfo metadataGroup = invocationManager.<CPGroupInfo>query(metadataGroupId, new GetRaftGroupOp(metadataGroupId), LEADER_LOCAL).get();
                CPGroupInfo otherGroup = invocationManager.<CPGroupInfo>query(metadataGroupId, new GetRaftGroupOp(groupId), LEADER_LOCAL).get();
                assertEquals(5, metadataGroup.memberCount());
                assertEquals(5, otherGroup.memberCount());

                assertNotNull(getRaftNode(instances[6], metadataGroupId));
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

        waitAllForLeaderElection(instances, INITIAL_METADATA_GROUP_ID);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNull(getRaftService(instance).getLocalMember());
            }
        });
    }

    @Test
    public void test_sessionClosedOnCPSubsystemReset() throws Exception {
        final HazelcastInstance[] instances = newInstances(3, 3, 1);

        instances[0].getCPSubsystem().getAtomicLong("long1").set(1);
        instances[0].getCPSubsystem().getAtomicLong("long1@custom").set(2);

        final FencedLock lock = instances[3].getCPSubsystem().getLock("lock");
        lock.lock();

        instances[0].getCPSubsystem().getCPSubsystemManagementService().restart().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                ProxySessionManagerService service = getNodeEngineImpl(instances[3]).getService(ProxySessionManagerService.SERVICE_NAME);
                assertEquals(NO_SESSION_ID, service.getSession((RaftGroupId) lock.getGroupId()));
            }
        });
    }

    @Test
    public void testNodesBecomeAP_whenMoreThanInitialRaftMembers_areStartedConcurrently() {
        final Config config = createConfig(4, 3);

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
                    if (getRaftGroupLocally(instance, getMetadataGroupId(instance)) != null) {
                        metadataCount++;
                    }
                }
                assertEquals(4, cpCount);
                assertEquals(3, metadataCount);
            }
        });
    }

    @Test
    public void testCPMemberIdentityChanges_whenLocalMemberIsRecovered_duringRestart() {
        final HazelcastInstance[] instances = newInstances(3);
        waitUntilCPDiscoveryCompleted(instances);
        waitAllForLeaderElection(instances, INITIAL_METADATA_GROUP_ID);

        Member localMember = instances[0].getCluster().getLocalMember();
        CPMember localCpMember = instances[0].getCPSubsystem().getLocalCPMember();
        instances[0].getLifecycleService().terminate();

        instances[0] = newHazelcastInstance(initOrCreateConfig(createConfig(3, 3)), randomString(),
                new StaticMemberNodeContext(factory, localMember));
        assertEquals(localMember, instances[0].getCluster().getLocalMember());

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertNull(instances[0].getCPSubsystem().getLocalCPMember());
            }
        }, 5);

        instances[0].getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(instances[0].getCPSubsystem().getLocalCPMember());
            }
        });
        assertNotEquals(localCpMember, getRaftService(instances[0]).getLocalMember());
    }

}
