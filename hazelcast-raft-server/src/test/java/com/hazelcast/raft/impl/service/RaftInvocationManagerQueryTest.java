package com.hazelcast.raft.impl.service;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.QueryPolicy.ANY_LOCAL;
import static com.hazelcast.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.service.RaftServiceUtil.getRaftNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftInvocationManagerQueryTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;

    @Test
    public void when_queryFromLeader_withoutAnyCommit_thenReadLinearizable() throws Exception {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        RaftInvocationManager invocationService = getRaftInvocationService(instances[0]);
        RaftGroupId groupId = invocationService.createRaftGroup(RaftDataService.SERVICE_NAME, "test", nodeCount);

        ICompletableFuture<Object> future = invocationService.query(groupId, new RaftTestQueryOperation(), LEADER_LOCAL);

        assertNull(future.get());

        RaftNodeImpl raftNode = getRaftNode(getLeaderInstance(instances, groupId), groupId);
        assertEquals(1, getCommitIndex(raftNode));
    }

    @Test
    public void when_queryFromFollower_withoutAnyCommit_thenReadLinearizable() throws Exception {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        RaftInvocationManager invocationService = getRaftInvocationService(instances[0]);
        RaftGroupId groupId = invocationService.createRaftGroup(RaftDataService.SERVICE_NAME, "test", nodeCount);

        ICompletableFuture<Object> future = invocationService.query(groupId, new RaftTestQueryOperation(), ANY_LOCAL);

        assertNull(future.get());

        RaftNodeImpl raftNode = getRaftNode(getLeaderInstance(instances, groupId), groupId);
        assertEquals(1, getCommitIndex(raftNode));
    }

    @Test
    public void when_queryFromLeader_onStableCluster_thenReadLatestValue() throws Exception {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        RaftInvocationManager invocationService = getRaftInvocationService(instances[0]);
        RaftGroupId groupId = invocationService.createRaftGroup(RaftDataService.SERVICE_NAME, "test", nodeCount);

        String value = "value";
        invocationService.invoke(groupId, new RaftTestApplyOperation(value)).get();

        ICompletableFuture<Object> future = invocationService.query(groupId, new RaftTestQueryOperation(), LEADER_LOCAL);

        assertEquals(value, future.get());
    }

    @Test
    public void when_queryFromFollower_onStableCluster_thenReadLatestValue() throws Exception {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        RaftInvocationManager invocationService = getRaftInvocationService(instances[0]);
        RaftGroupId groupId = invocationService.createRaftGroup(RaftDataService.SERVICE_NAME, "test", nodeCount);

        String value = "value";
        invocationService.invoke(groupId, new RaftTestApplyOperation(value)).get();

        ICompletableFuture<Object> future = invocationService.query(groupId, new RaftTestQueryOperation(), ANY_LOCAL);
        assertEquals(value, future.get());
    }

    @Test
    public void when_queryLocalFromFollower_withLeaderLocalPolicy_thenFail() throws Exception {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        RaftInvocationManager invocationService = getRaftInvocationService(instances[0]);
        RaftGroupId groupId = invocationService.createRaftGroup(RaftDataService.SERVICE_NAME, "test", nodeCount);

        String value = "value";
        invocationService.invoke(groupId, new RaftTestApplyOperation(value)).get();

        RaftNodeImpl leader = getLeaderNode(instances, groupId);
        HazelcastInstance followerInstance = getRandomFollowerInstance(instances, leader);
        RaftInvocationManager followerInvManager = getRaftInvocationService(followerInstance);

        ICompletableFuture<Object> future = followerInvManager.queryOnLocal(groupId, new RaftTestQueryOperation(), LEADER_LOCAL);
        try {
            future.get();
        } catch (ExecutionException e) {
            assertInstanceOf(NotLeaderException.class, e.getCause());
        }
    }

    @Test
    public void when_queryLocalFromLeader_withLeaderLocalPolicy_thenReadLatestValue() throws Exception {
        int nodeCount = 3;
        Address[] raftAddresses = createAddresses(nodeCount);
        instances = newInstances(raftAddresses);

        RaftInvocationManager invocationService = getRaftInvocationService(instances[0]);
        RaftGroupId groupId = invocationService.createRaftGroup(RaftDataService.SERVICE_NAME, "test", nodeCount);

        String value = "value";
        invocationService.invoke(groupId, new RaftTestApplyOperation(value)).get();

        HazelcastInstance leaderInstance = getLeaderInstance(instances, groupId);
        RaftInvocationManager leaderInvManager = getRaftInvocationService(leaderInstance);

        ICompletableFuture<Object> future = leaderInvManager.queryOnLocal(groupId, new RaftTestQueryOperation(), LEADER_LOCAL);
        assertEquals(value, future.get());
    }

    @Override
    protected Config createConfig(Address[] raftAddresses, int metadataGroupSize) {
        Config config = super.createConfig(raftAddresses, metadataGroupSize);

        ServiceConfig raftTestServiceConfig = new ServiceConfig().setEnabled(true)
                .setName(RaftDataService.SERVICE_NAME)
                .setClassName(RaftDataService.class.getName());
        config.getServicesConfig().addServiceConfig(raftTestServiceConfig);

        return config;
    }
}
