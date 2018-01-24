package com.hazelcast.raft.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.config.raft.RaftServiceConfig;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetActiveEndpointsOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetRaftGroupOp;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.impl.RaftUtil.waitUntilLeaderElected;
import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static com.hazelcast.raft.impl.service.RaftServiceUtil.getRaftNode;
import static com.hazelcast.raft.impl.service.RaftServiceUtil.getRaftService;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MetadataRaftClusterTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;

    @Test
    public void when_clusterStartsWithNonCPNodes_then_metadataClusterIsInitialized() {
        int cpNodeCount = 3;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, cpNodeCount,2);

        final List<Address> raftAddressesList = Arrays.asList(raftAddresses);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    if (raftAddressesList.contains(getAddress(instance))) {
                        assertNotNull(getRaftNode(instance, METADATA_GROUP_ID));
                    }
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    if (!raftAddressesList.contains(getAddress(instance))) {
                        assertNull(getRaftNode(instance, METADATA_GROUP_ID));
                    }
                }
            }
        }, 10);
    }

    @Test
    public void when_raftGroupIsCreatedWithAllCPNodes_then_raftNodeIsCreatedOnAll()  {
        int nodeCount = 5;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        final RaftGroupId groupId = createNewRaftGroup(instances[0], "id", nodeCount);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    assertNotNull(getRaftNode(instance, groupId));
                }
            }
        });
    }

    @Test
    public void when_raftGroupIsCreatedWithSomeCPNodes_then_raftNodeIsCreatedOnOnlyThem() {
        final int nodeCount = 5;
        final int metadataGroupSize = 3;
        final Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, 0);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (Address address : Arrays.asList(raftAddresses).subList(0, metadataGroupSize)) {
                    HazelcastInstance instance = factory.getInstance(address);
                    assertNotNull(getRaftNode(instance, METADATA_GROUP_ID));
                }
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                for (Address address : Arrays.asList(raftAddresses).subList(metadataGroupSize, raftAddresses.length)) {
                    HazelcastInstance instance = factory.getInstance(address);
                    assertNull(getRaftNode(instance, METADATA_GROUP_ID));
                }
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
        int cpNodeCount = 5;
        int metadataGroupSize = 2;
        int nonCpNodeCount = 2;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, nonCpNodeCount);

        final int newGroupCount = metadataGroupSize + 1;

        HazelcastInstance instance = instances[invokeOnCP ? 0 : instances.length - 1];
        final RaftGroupId groupId = createNewRaftGroup(instance, "id", newGroupCount);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int count = 0;
                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                    if (raftNode != null) {
                        count++;
                    }
                }

                assertEquals(newGroupCount, count);
            }
        });
    }

    @Test
    public void when_sizeOfRaftGroupIsLargerThanCPNodeCount_then_raftGroupCannotBeCreated() {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        try {
            createNewRaftGroup(instances[0], "id", nodeCount + 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void when_raftGroupIsCreatedWithSameSizeMultipleTimes_then_itSucceeds() {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);
        RaftGroupId groupId1 = createNewRaftGroup(instances[0], "id", nodeCount);
        RaftGroupId groupId2 = createNewRaftGroup(instances[1], "id", nodeCount);
        assertEquals(groupId1, groupId2);
    }

    @Test
    public void when_raftGroupIsCreatedWithDifferentSizeMultipleTimes_then_itFails() {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        createNewRaftGroup(instances[0], "id", nodeCount);
        try {
            createNewRaftGroup(instances[0], "id", nodeCount - 1);
            fail();
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void when_raftGroupDestroyTriggered_then_raftGroupIsDestroyed() {
        int metadataGroupSize = 3;
        int cpNodeCount = 5;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, 0);

        final RaftGroupId groupId = createNewRaftGroup(instances[0], "id", cpNodeCount);
        destroyRaftGroup(instances[0], groupId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    assertNull(getRaftNode(instance, groupId));
                }
            }
        });

        final RaftInvocationManager invocationService = getRaftInvocationService(instances[0]);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Future<RaftGroupInfo> f = invocationService.query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId),
                        QueryPolicy.LEADER_LOCAL);

                RaftGroupInfo groupInfo = f.get();
                assertEquals(RaftGroupStatus.DESTROYED, groupInfo.status());
            }
        });
    }

    @Test
    public void when_raftGroupDestroyTriggeredMultipleTimes_then_destroyDoesNotFail() {
        int metadataGroupSize = 3;
        int cpNodeCount = 5;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, 0);

        RaftGroupId groupId = createNewRaftGroup(instances[0], "id", cpNodeCount);
        destroyRaftGroup(instances[0], groupId);
        destroyRaftGroup(instances[0], groupId);
    }

    @Test
    public void when_raftGroupIsDestroyed_then_itCanBeCreatedAgain() {
        int metadataGroupSize = 3;
        int cpNodeCount = 5;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, 0);

        final RaftGroupId groupId = createNewRaftGroup(instances[0], "id", cpNodeCount);
        destroyRaftGroup(instances[0], groupId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    assertNull(getRaftNode(instance, groupId));
                }
            }
        });

        createNewRaftGroup(instances[0], "id", cpNodeCount - 1);
    }

    @Test
    public void when_metadataClusterNodeFallsFarBehind_then_itInstallsSnapshot() {
        int nodeCount = 3;
        int commitCountToSnapshot = 5;
        Address[] raftAddresses = createAddresses(nodeCount);
        Config config = createConfig(raftAddresses, raftAddresses.length);
        getRaftConfig(config).setCommitIndexAdvanceCountToSnapshot(commitCountToSnapshot);

        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(raftAddresses[i], config);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, METADATA_GROUP_ID);
                    assertNotNull(raftNode);
                }
            }
        });

        RaftEndpointImpl leaderEndpoint = getLeaderEndpoint(getRaftNode(instances[0], METADATA_GROUP_ID));
        final HazelcastInstance leader = factory.getInstance(leaderEndpoint.getAddress());
        HazelcastInstance follower = null;
        for (HazelcastInstance instance : instances) {
            if (!getAddress(instance).equals(leaderEndpoint.getAddress())) {
                follower = instance;
                break;
            }
        }

        assertNotNull(follower);
        blockCommunicationBetween(leader, follower);

        final List<RaftGroupId> groupIds = new ArrayList<RaftGroupId>();
        for (int i = 0; i < commitCountToSnapshot; i++) {
            RaftGroupId groupId = createNewRaftGroup(leader, "id" + i, nodeCount);
            groupIds.add(groupId);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getSnapshotEntry(getRaftNode(leader, METADATA_GROUP_ID)).index() > 0);
            }
        });

        unblockCommunicationBetween(leader, follower);

        final HazelcastInstance f = follower;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftGroupId groupId : groupIds) {
                    assertNotNull(getRaftNode(f, groupId));
                }
            }
        });
    }

    private static RaftConfig getRaftConfig(Config config) {
        RaftServiceConfig serviceConfig =
                (RaftServiceConfig) config.getServicesConfig().getServiceConfig(RaftService.SERVICE_NAME).getConfigObject();
        return serviceConfig.getRaftConfig();
    }

    @Test
    public void when_raftGroupIsCreated_onNonMetadataMembers_thenLeaderShouldBeElected()
            throws ExecutionException, InterruptedException {
        int metadataGroupSize = 3;
        int otherRaftGroupSize = 2;
        Address[] raftAddresses = createAddresses(metadataGroupSize + otherRaftGroupSize);
        instances = newInstances(raftAddresses, metadataGroupSize, 0);

        HazelcastInstance leaderInstance = getLeaderInstance(instances, METADATA_GROUP_ID);
        RaftService raftService = getRaftService(leaderInstance);
        Collection<RaftEndpointImpl> allEndpoints = raftService.getAllEndpoints();
        RaftGroupInfo metadataGroup = raftService.getRaftGroupInfo(METADATA_GROUP_ID);

        final Collection<RaftEndpointImpl> endpoints = new HashSet<RaftEndpointImpl>(otherRaftGroupSize);
        for (RaftEndpointImpl endpoint : allEndpoints) {
            if (!metadataGroup.containsMember(endpoint)) {
                endpoints.add(endpoint);
            }
        }
        assertEquals(otherRaftGroupSize, endpoints.size());

        ICompletableFuture<RaftGroupId> f = raftService.getInvocationManager()
                .invoke(METADATA_GROUP_ID, new CreateRaftGroupOp("test", endpoints));

        final RaftGroupId groupId = f.get();

        for (final HazelcastInstance instance : instances) {
            if (endpoints.contains(getRaftService(instance).getLocalEndpoint())) {
                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run() {
                        RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                        assertNotNull(raftNode);
                        waitUntilLeaderElected(raftNode);
                    }
                });
            }
        }
    }

    @Test
    public void when_shutdownLeader_thenNewLeaderElected() {
        int cpNodeCount = 5;
        int metadataGroupSize = cpNodeCount - 1;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, 0);

        HazelcastInstance leaderInstance = getLeaderInstance(instances, METADATA_GROUP_ID);
        leaderInstance.shutdown();

        for (HazelcastInstance instance : instances) {
            if (instance == leaderInstance) {
                continue;
            }
            RaftNodeImpl raftNode = getRaftNode(instance, METADATA_GROUP_ID);
            while (raftNode == null) {
                sleepMillis(100);
                raftNode = getRaftNode(instance, METADATA_GROUP_ID);
            }
            waitUntilLeaderElected(raftNode);
        }
    }

    @Test
    public void when_memberIsShutdown_then_itIsRemovedFromRaftGroups()
            throws ExecutionException, InterruptedException {
        int cpNodeCount = 5;
        int metadataGroupSize = cpNodeCount - 1;
        int atomicLong1GroupSize = cpNodeCount - 2;
        Address[] raftAddresses = createAddresses(cpNodeCount);
        instances = newInstances(raftAddresses, metadataGroupSize, 0);

        final RaftGroupId groupId1 = createNewRaftGroup(instances[0], "id1", atomicLong1GroupSize);
        final RaftGroupId groupId2 = createNewRaftGroup(instances[0], "id2", cpNodeCount);

        RaftEndpointImpl endpoint = findCommonEndpoint(instances[0], METADATA_GROUP_ID, groupId1);
        assertNotNull(endpoint);

        RaftInvocationManager invocationService = null;
        for (HazelcastInstance instance : instances) {
            if (!getAddress(instance).equals(endpoint.getAddress())) {
                invocationService = getRaftInvocationService(instance);
                break;
            }
        }
        assertNotNull(invocationService);

        factory.getInstance(endpoint.getAddress()).shutdown();

        ICompletableFuture<List<RaftEndpointImpl>> f1 = invocationService.query(METADATA_GROUP_ID, new GetActiveEndpointsOp(),
                QueryPolicy.LEADER_LOCAL);

        List<RaftEndpointImpl> activeEndpoints = f1.get();
        assertThat(activeEndpoints, not(hasItem(endpoint)));

        ICompletableFuture<RaftGroupInfo> f2 = invocationService.query(METADATA_GROUP_ID, new GetRaftGroupOp(METADATA_GROUP_ID),
                QueryPolicy.LEADER_LOCAL);

        ICompletableFuture<RaftGroupInfo> f3 = invocationService.query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId1),
                QueryPolicy.LEADER_LOCAL);

        ICompletableFuture<RaftGroupInfo> f4 = invocationService.query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId2),
                QueryPolicy.LEADER_LOCAL);

        RaftGroupInfo metadataGroup = f2.get();
        assertFalse(metadataGroup.containsMember(endpoint));
        assertEquals(metadataGroupSize, metadataGroup.memberCount());
        RaftGroupInfo atomicLongGroup1 = f3.get();
        assertFalse(atomicLongGroup1.containsMember(endpoint));
        assertEquals(atomicLong1GroupSize, atomicLongGroup1.memberCount());
        RaftGroupInfo atomicLongGroup2 = f4.get();
        assertFalse(atomicLongGroup2.containsMember(endpoint));
        assertEquals(cpNodeCount - 1, atomicLongGroup2.memberCount());
    }

    private RaftGroupId createNewRaftGroup(HazelcastInstance instance, String name, int nodeCount) {
        RaftInvocationManager invocationManager = getRaftService(instance).getInvocationManager();
        try {
            return invocationManager.createRaftGroup(name, nodeCount).get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void destroyRaftGroup(HazelcastInstance instance, RaftGroupId groupId) {
        RaftInvocationManager invocationManager = getRaftService(instance).getInvocationManager();
        try {
            invocationManager.triggerDestroyRaftGroup(groupId).get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private RaftEndpointImpl findCommonEndpoint(HazelcastInstance instance, final RaftGroupId groupId1, final RaftGroupId groupId2)
            throws ExecutionException, InterruptedException {
        RaftInvocationManager invocationService = getRaftInvocationService(instance);
        ICompletableFuture<RaftGroupInfo> f1 = invocationService.query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId1),
                QueryPolicy.LEADER_LOCAL);
        ICompletableFuture<RaftGroupInfo> f2 = invocationService.query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId2),
                QueryPolicy.LEADER_LOCAL);
        RaftGroupInfo group1 = f1.get();
        RaftGroupInfo group2 = f2.get();

        Set<RaftEndpointImpl> members = new HashSet<RaftEndpointImpl>(group1.endpointImpls());
        members.retainAll(group2.members());

        return members.isEmpty() ? null : members.iterator().next();
    }

    @Override
    protected Config createConfig(Address[] raftAddresses, int metadataGroupSize) {
        Config config = super.createConfig(raftAddresses, metadataGroupSize);
        getRaftConfig(config).setAppendNopEntryOnLeaderElection(true).setLeaderHeartbeatPeriodInMillis(1000);
        return config;
    }

}
