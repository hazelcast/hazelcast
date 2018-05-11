package com.hazelcast.raft.impl.service;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftInvocationManagerQueryTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;

    @Test
    public void when_queryFromLeader_withoutAnyCommit_thenReturnDefaultValue() throws Exception {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        RaftGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        ICompletableFuture<Object> future = invocationService.query(groupId, new RaftTestQueryOp(), LEADER_LOCAL);
        assertNull(future.get());
    }

    @Test
    public void when_queryFromFollower_withoutAnyCommit_thenReturnDefaultValue() throws Exception {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        RaftGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        ICompletableFuture<Object> future = invocationService.query(groupId, new RaftTestQueryOp(), ANY_LOCAL);
        assertNull(future.get());
    }

    @Test
    public void when_queryFromLeader_onStableCluster_thenReadLatestValue() throws Exception {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        RaftGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        String value = "value";
        invocationService.invoke(groupId, new RaftTestApplyOp(value)).get();

        ICompletableFuture<Object> future = invocationService.query(groupId, new RaftTestQueryOp(), LEADER_LOCAL);

        assertEquals(value, future.get());
    }

    @Test
    public void when_queryFromFollower_onStableCluster_thenReadLatestValue() throws Exception {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        RaftGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        String value = "value";
        invocationService.invoke(groupId, new RaftTestApplyOp(value)).get();

        ICompletableFuture<Object> future = invocationService.query(groupId, new RaftTestQueryOp(), ANY_LOCAL);
        assertEquals(value, future.get());
    }

    @Test
    public void when_queryLocalFromFollower_withLeaderLocalPolicy_thenFail() throws Exception {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        RaftGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        String value = "value";
        invocationService.invoke(groupId, new RaftTestApplyOp(value)).get();

        RaftNodeImpl leader = getLeaderNode(instances, groupId);
        HazelcastInstance followerInstance = getRandomFollowerInstance(instances, leader);
        RaftInvocationManager followerInvManager = getRaftInvocationManager(followerInstance);

        ICompletableFuture<Object> future = followerInvManager.queryOnLocal(groupId, new RaftTestQueryOp(), LEADER_LOCAL);
        try {
            future.get();
        } catch (ExecutionException e) {
            assertInstanceOf(NotLeaderException.class, e.getCause());
        }
    }

    @Test
    public void when_queryLocalFromLeader_withLeaderLocalPolicy_thenReadLatestValue() throws Exception {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        RaftGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        String value = "value";
        invocationService.invoke(groupId, new RaftTestApplyOp(value)).get();

        HazelcastInstance leaderInstance = getLeaderInstance(instances, groupId);
        RaftInvocationManager leaderInvManager = getRaftInvocationManager(leaderInstance);

        ICompletableFuture<Object> future = leaderInvManager.queryOnLocal(groupId, new RaftTestQueryOp(), LEADER_LOCAL);
        assertEquals(value, future.get());
    }
}
