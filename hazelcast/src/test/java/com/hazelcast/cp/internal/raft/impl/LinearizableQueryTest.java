/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.cp.exception.CannotReplicateException;
import com.hazelcast.cp.exception.LeaderDemotedException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dataservice.QueryRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.cp.internal.raft.impl.RaftRole.LEADER;
import static com.hazelcast.cp.internal.raft.MembershipChangeMode.REMOVE;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastGroupMembers;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getRole;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderQueryRound;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LinearizableQueryTest extends HazelcastTestSupport {

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

    @Test(timeout = 300_000)
    public void when_linearizableQueryIsIssued_then_itReadsLastState() throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new ApplyRaftRunnable("value1")).get();
        long commitIndex1 = getCommitIndex(leader);

        Object o1 = leader.query(new QueryRaftRunnable(), LINEARIZABLE).get();

        assertEquals("value1", o1);
        long leaderQueryRound1 = getLeaderQueryRound(leader);
        assertTrue(leaderQueryRound1 > 0);
        assertEquals(commitIndex1, getCommitIndex(leader));

        leader.replicate(new ApplyRaftRunnable("value2")).get();
        long commitIndex2 = getCommitIndex(leader);

        Object o2 = leader.query(new QueryRaftRunnable(), LINEARIZABLE).get();

        assertEquals("value2", o2);
        long leaderQueryRound2 = getLeaderQueryRound(leader);
        assertEquals(leaderQueryRound1 + 1, leaderQueryRound2);
        assertEquals(commitIndex2, getCommitIndex(leader));
    }

    @Test(timeout = 300_000)
    public void when_newCommitIsDoneWhileThereIsWaitingQuery_then_queryRunsAfterNewCommit() throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new ApplyRaftRunnable("value1")).get();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        group.dropMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[2].getLocalMember(), AppendRequest.class);

        InternalCompletableFuture replicateFuture = leader.replicate(new ApplyRaftRunnable("value2"));
        InternalCompletableFuture queryFuture = leader.query(new QueryRaftRunnable(), LINEARIZABLE);

        group.resetAllRulesFrom(leader.getLocalMember());

        replicateFuture.get();
        Object o = queryFuture.get();
        assertEquals("value2", o);
    }

    @Test(timeout = 300_000)
    public void when_multipleQueriesAreIssuedBeforeHeartbeatAcksReceived_then_allQueriesExecutedAtOnce() throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new ApplyRaftRunnable("value1")).get();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        group.dropMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[2].getLocalMember(), AppendRequest.class);

        InternalCompletableFuture queryFuture1 = leader.query(new QueryRaftRunnable(), LINEARIZABLE);
        InternalCompletableFuture queryFuture2 = leader.query(new QueryRaftRunnable(), LINEARIZABLE);

        group.resetAllRulesFrom(leader.getLocalMember());

        assertEquals("value1", queryFuture1.get());
        assertEquals("value1", queryFuture2.get());
    }

    @Test(timeout = 300_000)
    public void when_newCommitIsDoneWhileThereAreMultipleQueries_then_allQueriesRunAfterCommit() throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new ApplyRaftRunnable("value1")).get();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        group.dropMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[2].getLocalMember(), AppendRequest.class);

        InternalCompletableFuture replicateFuture = leader.replicate(new ApplyRaftRunnable("value2"));
        InternalCompletableFuture queryFuture1 = leader.query(new QueryRaftRunnable(), LINEARIZABLE);
        InternalCompletableFuture queryFuture2 = leader.query(new QueryRaftRunnable(), LINEARIZABLE);

        group.resetAllRulesFrom(leader.getLocalMember());

        replicateFuture.get();
        assertEquals("value2", queryFuture1.get());
        assertEquals("value2", queryFuture2.get());
    }

    @Test(timeout = 300_000)
    public void when_linearizableQueryIsIssuedToFollower_then_queryFails() {
        group = newGroup();
        group.start();

        group.waitUntilLeaderElected();
        try {
            group.getAnyFollowerNode().query(new QueryRaftRunnable(), LINEARIZABLE).joinInternal();
            fail();
        } catch (NotLeaderException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void when_multipleQueryLimitIsReachedBeforeHeartbeatAcks_then_noNewQueryIsAccepted() throws Exception {
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setUncommittedEntryCountToRejectNewAppends(1);
        group = new LocalRaftGroupBuilder(5, config).setAppendNopEntryOnLeaderElection(true).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        assertTrueEventually(() -> assertThat(getCommitIndex(leader)).isGreaterThanOrEqualTo(1L));

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        group.dropMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[2].getLocalMember(), AppendRequest.class);

        InternalCompletableFuture queryFuture1 = leader.query(new QueryRaftRunnable(), LINEARIZABLE);
        InternalCompletableFuture queryFuture2 = leader.query(new QueryRaftRunnable(), LINEARIZABLE);

        try {
            queryFuture2.joinInternal();
            fail();
        } catch (CannotReplicateException ignored) {
        }

        group.resetAllRulesFrom(leader.getLocalMember());

        queryFuture1.get();
    }

    @Test(timeout = 300_000)
    public void when_leaderDemotesToFollowerWhileThereIsOngoingQuery_then_queryFails() throws Exception {
        group = newGroup();
        group.start();

        RaftNodeImpl oldLeader = group.waitUntilLeaderElected();

        final int[] split = group.createMajoritySplitIndexes(false);
        group.split(split);

        InternalCompletableFuture queryFuture = oldLeader.query(new QueryRaftRunnable(), LINEARIZABLE);

        assertTrueEventually(() -> {
            for (int ix : split) {
                RaftEndpoint newLeader = getLeaderMember(group.getNode(ix));
                assertNotNull(newLeader);
                assertNotEquals(oldLeader.getLocalMember(), newLeader);
            }
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(group.getNode(split[0])));
        newLeader.replicate(new ApplyRaftRunnable("value1")).get();

        group.merge();
        group.waitUntilLeaderElected();

        assertEquals(oldLeader.getLeader(), newLeader.getLocalMember());
        try {
            queryFuture.joinInternal();
            fail();
        } catch (LeaderDemotedException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void when_leaderIsLeaving_then_itDoesNotServeNewLinearizableQuery() throws Exception {
        group = LocalRaftGroupBuilder.newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftEndpoint[] followers = group.getFollowerEndpoints();
        leader.replicate(new ApplyRaftRunnable("value1")).get();

        // Only one of the two remaining members can acknowledge the leader from now on, so its
        // own removal cannot commit, and it stays LEADER with the new member list already applied.
        group.dropMessagesToMember(followers[1], leader.getLocalMember(), AppendSuccessResponse.class);
        InternalCompletableFuture<?> removal = leader.replicateMembershipChange(leader.getLocalMember(), REMOVE);

        // The new member list is locally applied, and the "leader" is no longer in the member list
        assertTrueEventually(() -> assertFalse(getLastGroupMembers(leader).isKnownMember(leader.getLocalMember())));
        // The removal is accepted but not committed, so the leaving state holds for the rest of the test
        assertFalse(removal.isDone());
        // The role is kept so the removal can finish replicating, hence leader-only reads still arrive here
        assertEquals(LEADER, getRole(leader));

        InternalCompletableFuture<Object> query = leader.query(new QueryRaftRunnable(), LINEARIZABLE);

        // The LINEARIZABLE read needs a heartbeat round that the leaving leader cannot be part of,
        // since it is no longer in the member list and should not count its own acknowledgement. The
        // query is never admitted, and is rejected the same way a new append is rejected while the
        // leader is stepping down.
        assertQueryFailedWith(CannotReplicateException.class, query);
    }

    @Test(timeout = 300_000)
    public void when_leaderStartsLeaving_then_itDoesNotCompletePendingLinearizableQuery() throws Exception {
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(500);
        group = LocalRaftGroupBuilder.newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftEndpoint[] followers = group.getFollowerEndpoints();
        leader.replicate(new ApplyRaftRunnable("value1")).get();

        // Hold back every acknowledgement, so the query is admitted while the leader is still
        // a full member and is then left waiting for its heartbeat round.
        group.dropMessagesToMember(followers[0], leader.getLocalMember(), AppendSuccessResponse.class);
        group.dropMessagesToMember(followers[1], leader.getLocalMember(), AppendSuccessResponse.class);

        InternalCompletableFuture<Object> query = leader.query(new QueryRaftRunnable(), LINEARIZABLE);
        assertTrueAllTheTime(() -> assertFalse(query.isDone()), 2);

        InternalCompletableFuture<?> removal = leader.replicateMembershipChange(leader.getLocalMember(), REMOVE);

        // The new member list is locally applied, and the "leader" is no longer in the member list
        assertTrueEventually(() -> assertFalse(getLastGroupMembers(leader).isKnownMember(leader.getLocalMember())));
        // The removal is accepted but not committed, so the leaving state holds for the rest of the test
        assertFalse(removal.isDone());
        // The role is kept so the removal can finish replicating
        assertEquals(LEADER, getRole(leader));

        // Let a single follower through. Its acknowledgement should not be added to the leaving
        // leader's own, since the leader is no longer in the current member list. So the query
        // admitted before the transition should have failed.
        group.allowMessagesToMember(followers[0], leader.getLocalMember(), AppendSuccessResponse.class);

        assertQueryFailedWith(LeaderDemotedException.class, query);
    }

    @Test(timeout = 300_000)
    public void when_anotherMemberIsLeaving_then_leaderStillServesLinearizableQuery() throws Exception {
        group = LocalRaftGroupBuilder.newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftEndpoint[] followers = group.getFollowerEndpoints();
        leader.replicate(new ApplyRaftRunnable("value1")).get();

        // Hold back the removal log entries while still delivering heartbeats and query rounds,
        // so that the removal of another member stays uncommitted for the rest of the test.
        holdBackLogEntries(leader, followers[0]);
        holdBackLogEntries(leader, followers[1]);

        InternalCompletableFuture<?> removal = leader.replicateMembershipChange(followers[0], REMOVE);

        // The new member list is locally applied without the leaving member, and the leader is
        // still one of its members, so it may count its own acknowledgement
        assertTrueEventually(() -> assertFalse(getLastGroupMembers(leader).isKnownMember(followers[0])));
        assertTrue(getLastGroupMembers(leader).isKnownMember(leader.getLocalMember()));
        // The removal is accepted but not committed, so the leaving state holds for the rest of the test
        assertFalse(removal.isDone());
        assertEquals(LEADER, getRole(leader));

        // Rejecting the reads of a leaving leader must not spread to any other member's removal.
        assertEquals("value1", leader.query(new QueryRaftRunnable(), LINEARIZABLE).get());
    }

    /**
     * Replaces the append requests that carry log entries with plain heartbeats, so that the
     * follower keeps acknowledging query rounds without ever appending the entries.
     */
    private void holdBackLogEntries(RaftNodeImpl leader, RaftEndpoint follower) {
        group.alterMessagesToMember(leader.getLocalMember(), follower, message -> {
            if (!(message instanceof AppendRequest request) || request.entryCount() == 0) {
                return message;
            }
            return new AppendRequest(request.leader(), request.term(), request.prevLogTerm(),
                    request.prevLogIndex(), request.leaderCommitIndex(), new LogEntry[0],
                    request.queryRound(), request.flowControlSequenceNumber());
        });
    }

    private static void assertQueryFailedWith(Class<? extends Throwable> expected,
                                              InternalCompletableFuture<?> query) {
        Object result;
        try {
            result = query.joinInternal();
        } catch (Throwable t) {
            assertEquals(expected, t.getClass());
            return;
        }
        fail("the query was served with: " + result + " instead of failing with " + expected.getSimpleName());
    }

    private LocalRaftGroup newGroup() {
        return new LocalRaftGroupBuilder(5).setAppendNopEntryOnLeaderElection(true).build();
    }

}
