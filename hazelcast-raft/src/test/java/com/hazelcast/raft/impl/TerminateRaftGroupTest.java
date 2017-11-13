package com.hazelcast.raft.impl;

import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.service.RaftTestApplyOperation;
import com.hazelcast.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.raft.operation.TerminateRaftGroupOp;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.impl.RaftUtil.getStatus;
import static com.hazelcast.raft.impl.RaftUtil.newGroupWithService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TerminateRaftGroupTest extends HazelcastTestSupport {

    private LocalRaftGroup group;

    @Before
    public void init() {
    }

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void when_terminateOpIsAppendedButNotCommitted_then_cannotAppendNewEntry() throws ExecutionException, InterruptedException {
        group = newGroupWithService(2, new RaftConfig());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();

        group.dropAllMessagesToEndpoint(leader.getLocalEndpoint(), follower.getLocalEndpoint());

        leader.replicate(new TerminateRaftGroupOp());

        try {
            leader.replicate(new RaftTestApplyOperation("val")).get();
            fail();
        } catch (CannotReplicateException ignored) {
        }
    }

    @Test
    public void when_terminateOpIsAppended_then_statusIsTerminating() {
        group = newGroupWithService(2, new RaftConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl follower = group.getAnyFollowerNode();

        group.dropAllMessagesToEndpoint(follower.getLocalEndpoint(), leader.getLocalEndpoint());

        leader.replicate(new TerminateRaftGroupOp());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(RaftNodeStatus.TERMINATING, getStatus(leader));
                assertEquals(RaftNodeStatus.TERMINATING, getStatus(follower));
            }
        });
    }

    @Test
    public void when_terminateOpIsCommitted_then_raftNodeIsTerminated() throws ExecutionException, InterruptedException {
        group = newGroupWithService(2, new RaftConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl follower = group.getAnyFollowerNode();

        leader.replicate(new TerminateRaftGroupOp()).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, getCommitIndex(leader));
                assertEquals(1, getCommitIndex(follower));
                assertEquals(RaftNodeStatus.TERMINATED, getStatus(leader));
                assertEquals(RaftNodeStatus.TERMINATED, getStatus(follower));
            }
        });

        try {
            leader.replicate(new RaftTestApplyOperation("val")).get();
            fail();
        } catch (RaftGroupTerminatedException ignored) {

        }

        try {
            follower.replicate(new RaftTestApplyOperation("val")).get();
            fail();
        } catch (RaftGroupTerminatedException ignored) {
        }
    }

    @Test
    public void when_terminateOpIsTruncated_then_statusIsActive() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

        group.dropMessagesToAll(leader.getLocalEndpoint(), AppendRequest.class);

        leader.replicate(new TerminateRaftGroupOp());

        group.split(leader.getLocalEndpoint());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : followers) {
                    RaftEndpoint leaderEndpoint = getLeaderEndpoint(raftNode);
                    assertNotNull(leaderEndpoint);
                    assertNotEquals(leader.getLocalEndpoint(), leaderEndpoint);
                }
            }
        });

        final RaftNodeImpl newLeader = group.getNode(getLeaderEndpoint(followers[0]));

        for (int i = 0; i < 10; i++) {
            newLeader.replicate(new RaftTestApplyOperation("val" + i)).get();
        }

        group.merge();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(RaftNodeStatus.ACTIVE, getStatus(raftNode));
                }
            }
        });
    }

}
