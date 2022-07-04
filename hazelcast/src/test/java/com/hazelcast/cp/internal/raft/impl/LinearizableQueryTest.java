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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.cp.exception.CannotReplicateException;
import com.hazelcast.cp.exception.LeaderDemotedException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dataservice.QueryRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderQueryRound;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
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
    public void when_linearizableQueryIsIssuedToFollower_then_queryFails() throws Exception {
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
        assertTrueEventually(() -> assertThat(getCommitIndex(leader), greaterThanOrEqualTo(1L)));

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

    private LocalRaftGroup newGroup() {
        return new LocalRaftGroupBuilder(5).setAppendNopEntryOnLeaderElection(true).build();
    }

}
