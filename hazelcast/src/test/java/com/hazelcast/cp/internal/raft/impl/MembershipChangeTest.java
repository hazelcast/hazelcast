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
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.exception.MemberAlreadyExistsException;
import com.hazelcast.cp.internal.raft.exception.MemberDoesNotExistException;
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dataservice.RaftDataService;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.state.RaftGroupMembers;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder;
import com.hazelcast.cp.internal.raft.impl.testing.RaftRunnable;
import com.hazelcast.cp.internal.raft.impl.util.PostponedResponse;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.cp.internal.raft.MembershipChangeMode.REMOVE;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommittedGroupMembers;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastGroupMembers;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getStatus;
import static com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder.newGroup;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MembershipChangeTest extends HazelcastTestSupport {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void when_newRaftNodeJoins_then_itAppendsMissingEntries() throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new ApplyRaftRunnable("val")).get();

        RaftNodeImpl newRaftNode = group.createNewRaftNode();

        leader.replicateMembershipChange(newRaftNode.getLocalMember(), MembershipChangeMode.ADD).get();

        long commitIndex = getCommitIndex(leader);
        assertTrueEventually(() -> assertEquals(commitIndex, getCommitIndex(newRaftNode)));

        RaftGroupMembers lastGroupMembers = RaftUtil.getLastGroupMembers(leader);
        assertTrueEventually(() -> {
            RaftNodeImpl[] nodes = group.getNodes();
            for (RaftNodeImpl raftNode : nodes) {
                assertEquals(RaftNodeStatus.ACTIVE, getStatus(raftNode));
                assertEquals(lastGroupMembers.members(), getLastGroupMembers(raftNode).members());
                assertEquals(lastGroupMembers.index(), getLastGroupMembers(raftNode).index());
                assertEquals(lastGroupMembers.members(), getCommittedGroupMembers(raftNode).members());
                assertEquals(lastGroupMembers.index(), getCommittedGroupMembers(raftNode).index());
            }
        });

        RaftDataService service = group.getService(newRaftNode);
        assertEquals(1, service.size());
        assertTrue(service.values().contains("val"));
    }

    @Test
    public void when_followerLeaves_then_itIsRemovedFromTheGroupMembers() throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl leavingFollower = followers[0];
        RaftNodeImpl stayingFollower = followers[1];

        leader.replicate(new ApplyRaftRunnable("val")).get();

        leader.replicateMembershipChange(leavingFollower.getLocalMember(), REMOVE).get();

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : asList(leader, stayingFollower)) {
                assertFalse(getLastGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalMember()));
                assertFalse(getCommittedGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalMember()));
            }
        });

        group.terminateNode(leavingFollower.getLocalMember());
    }

    @Test
    public void when_newRaftNodeJoinsAfterAnotherNodeLeaves_then_itAppendsMissingEntries() throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new ApplyRaftRunnable("val")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl leavingFollower = followers[0];
        RaftNodeImpl stayingFollower = followers[1];

        leader.replicateMembershipChange(leavingFollower.getLocalMember(), REMOVE).get();

        RaftNodeImpl newRaftNode = group.createNewRaftNode();

        leader.replicateMembershipChange(newRaftNode.getLocalMember(), MembershipChangeMode.ADD).get();

        long commitIndex = getCommitIndex(leader);
        assertTrueEventually(() -> assertEquals(commitIndex, getCommitIndex(newRaftNode)));

        RaftGroupMembers lastGroupMembers = RaftUtil.getLastGroupMembers(leader);
        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : asList(leader, stayingFollower, newRaftNode)) {
                assertEquals(RaftNodeStatus.ACTIVE, getStatus(raftNode));
                assertEquals(lastGroupMembers.members(), getLastGroupMembers(raftNode).members());
                assertEquals(lastGroupMembers.index(), getLastGroupMembers(raftNode).index());
                assertEquals(lastGroupMembers.members(), getCommittedGroupMembers(raftNode).members());
                assertEquals(lastGroupMembers.index(), getCommittedGroupMembers(raftNode).index());
                assertFalse(getLastGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalMember()));
                assertFalse(getCommittedGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalMember()));
            }
        });

        RaftDataService service = group.getService(newRaftNode);
        assertEquals(1, service.size());
        assertTrue(service.values().contains("val"));
    }

    @Test
    public void when_newRaftNodeJoinsAfterAnotherNodeLeavesAndSnapshotIsTaken_then_itAppendsMissingEntries() throws ExecutionException, InterruptedException {
        int commitIndexAdvanceCountToSnapshot = 10;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCountToSnapshot);
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new ApplyRaftRunnable("val")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl leavingFollower = followers[0];
        RaftNodeImpl stayingFollower = followers[1];

        leader.replicateMembershipChange(leavingFollower.getLocalMember(), REMOVE).get();

        for (int i = 0; i < commitIndexAdvanceCountToSnapshot; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> assertTrue(getSnapshotEntry(leader).index() > 0));

        RaftNodeImpl newRaftNode = group.createNewRaftNode();

        leader.replicateMembershipChange(newRaftNode.getLocalMember(), MembershipChangeMode.ADD).get();

        long commitIndex = getCommitIndex(leader);
        assertTrueEventually(() -> assertEquals(commitIndex, getCommitIndex(newRaftNode)));

        RaftGroupMembers lastGroupMembers = RaftUtil.getLastGroupMembers(leader);
        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : asList(leader, stayingFollower, newRaftNode)) {
                assertEquals(RaftNodeStatus.ACTIVE, getStatus(raftNode));
                assertEquals(lastGroupMembers.members(), getLastGroupMembers(raftNode).members());
                assertEquals(lastGroupMembers.index(), getLastGroupMembers(raftNode).index());
                assertEquals(lastGroupMembers.members(), getCommittedGroupMembers(raftNode).members());
                assertEquals(lastGroupMembers.index(), getCommittedGroupMembers(raftNode).index());
                assertFalse(getLastGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalMember()));
                assertFalse(getCommittedGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalMember()));
            }
        });

        RaftDataService service = group.getService(newRaftNode);
        assertEquals(commitIndexAdvanceCountToSnapshot + 1, service.size());
        assertTrue(service.values().contains("val"));
        for (int i = 0; i < commitIndexAdvanceCountToSnapshot; i++) {
            assertTrue(service.values().contains("val" + i));
        }
    }

    @Test
    public void when_leaderLeaves_then_itIsRemovedFromTheGroupMembers() throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(leader.getLocalMember(), REMOVE).get();

        assertEquals(RaftNodeStatus.STEPPED_DOWN, getStatus(leader));

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertFalse(getLastGroupMembers(raftNode).isKnownMember(leader.getLocalMember()));
                assertFalse(getCommittedGroupMembers(raftNode).isKnownMember(leader.getLocalMember()));
            }
        });
    }

    @Test
    public void when_leaderLeaves_then_itCannotVoteForCommitOfMemberChange() throws ExecutionException, InterruptedException {
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(1000);
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        group.dropMessagesToMember(followers[0].getLocalMember(), leader.getLocalMember(), AppendSuccessResponse.class);
        leader.replicate(new ApplyRaftRunnable("val")).get();

        leader.replicateMembershipChange(leader.getLocalMember(), MembershipChangeMode.REMOVE);

        assertTrueAllTheTime(() -> assertEquals(1, getCommitIndex(leader)), 10);
    }

    @Test
    public void when_leaderLeaves_then_followersElectNewLeader() throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(leader.getLocalMember(), REMOVE).get();

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertFalse(getLastGroupMembers(raftNode).isKnownMember(leader.getLocalMember()));
                assertFalse(getCommittedGroupMembers(raftNode).isKnownMember(leader.getLocalMember()));
            }
        });

        group.terminateNode(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint newLeader = getLeaderMember(raftNode);
                assertNotNull(newLeader);
                assertNotEquals(leader.getLocalMember(), newLeader);
            }
        });
    }

    @Test
    public void when_membershipChangeRequestIsMadeWithWrongType_then_theChangeFails() throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new ApplyRaftRunnable("val")).get();

        try {
            leader.replicateMembershipChange(leader.getLocalMember(), null).get();
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void when_nonExistingEndpointIsRemoved_then_theChangeFails() throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl leavingFollower = group.getAnyFollowerNode();

        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(leavingFollower.getLocalMember(), MembershipChangeMode.REMOVE).get();

        try {
            leader.replicateMembershipChange(leavingFollower.getLocalMember(), MembershipChangeMode.REMOVE).joinInternal();
            fail();
        } catch (MemberDoesNotExistException ignored) {
        }
    }

    @Test
    public void when_existingEndpointIsAdded_then_theChangeFails() throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new ApplyRaftRunnable("val")).get();

        try {
            leader.replicateMembershipChange(leader.getLocalMember(), MembershipChangeMode.ADD).joinInternal();
            fail();
        } catch (MemberAlreadyExistsException ignored) {
        }
    }

    @Test
    public void when_thereIsNoCommitInTheCurrentTerm_then_cannotMakeMemberChange() throws ExecutionException, InterruptedException {
        // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        try {
            leader.replicateMembershipChange(leader.getLocalMember(), MembershipChangeMode.REMOVE).joinInternal();
            fail();
        } catch (CannotReplicateException ignored) {
        }
    }

    @Test
    public void when_appendNopEntryOnLeaderElection_then_canMakeMemberChangeAfterNopEntryCommitted() {
        // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

        group = new LocalRaftGroupBuilder(3).setAppendNopEntryOnLeaderElection(true).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        assertTrueEventually(() -> {
            // may fail until nop-entry is committed
            try {
                leader.replicateMembershipChange(leader.getLocalMember(), MembershipChangeMode.REMOVE).get();
            } catch (CannotReplicateException e) {
                fail(e.getMessage());
            }
        });
    }

    @Test
    public void when_newJoiningNodeFirstReceivesSnapshot_then_itInstallsSnapshot() throws ExecutionException, InterruptedException {
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(5);
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        for (int i = 0; i < 4; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        RaftNodeImpl newRaftNode = group.createNewRaftNode();

        group.dropMessagesToMember(leader.getLocalMember(), newRaftNode.getLocalMember(), AppendRequest.class);

        leader.replicateMembershipChange(newRaftNode.getLocalMember(), MembershipChangeMode.ADD).get();

        assertTrueEventually(() -> assertTrue(getSnapshotEntry(leader).index() > 0));

        group.resetAllRulesFrom(leader.getLocalMember());

        assertTrueEventually(() -> {
            assertEquals(getCommitIndex(leader), getCommitIndex(newRaftNode));
            assertEquals(getLastGroupMembers(leader).members(), getLastGroupMembers(newRaftNode).members());
            assertEquals(getLastGroupMembers(leader).members(), getCommittedGroupMembers(newRaftNode).members());
            RaftDataService service = group.getService(newRaftNode);
            assertEquals(4, service.size());
        });
    }

    @Test
    public void when_leaderFailsWhileLeavingRaftGroup_othersCommitTheMemberChange() throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        leader.replicate(new ApplyRaftRunnable("val")).get();

        for (RaftNodeImpl follower : followers) {
            group.dropMessagesToMember(follower.getLocalMember(), leader.getLocalMember(), AppendSuccessResponse.class);
            group.dropMessagesToMember(follower.getLocalMember(), leader.getLocalMember(), AppendFailureResponse.class);
        }

        leader.replicateMembershipChange(leader.getLocalMember(), MembershipChangeMode.REMOVE);

        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertEquals(2, getLastLogOrSnapshotEntry(follower).index());
            }
        });

        group.terminateNode(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : followers) {
                RaftEndpoint newLeaderEndpoint = getLeaderMember(follower);
                assertNotNull(newLeaderEndpoint);
                assertNotEquals(leader.getLocalMember(), newLeaderEndpoint);
            }
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));
        newLeader.replicate(new ApplyRaftRunnable("val2"));

        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertFalse(getCommittedGroupMembers(follower).isKnownMember(leader.getLocalMember()));
            }
        });
    }

    @Test
    public void when_followerAppendsMultipleMembershipChangesAtOnce_then_itCommitsThemCorrectly() throws ExecutionException, InterruptedException {
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(1000);
        group = newGroup(5, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        leader.replicate(new ApplyRaftRunnable("val")).get();

        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertEquals(1L, getCommitIndex(follower));
            }
        });

        RaftNodeImpl slowFollower = followers[0];

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.dropMessagesToMember(follower.getLocalMember(), follower.getLeader(), AppendSuccessResponse.class);
                group.dropMessagesToMember(follower.getLocalMember(), follower.getLeader(), AppendFailureResponse.class);
            }
        }

        RaftNodeImpl newRaftNode1 = group.createNewRaftNode();
        group.dropMessagesToMember(leader.getLocalMember(), newRaftNode1.getLocalMember(), AppendRequest.class);
        Future f1 = leader.replicateMembershipChange(newRaftNode1.getLocalMember(), MembershipChangeMode.ADD);

        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertEquals(2L, getLastLogOrSnapshotEntry(follower).index());
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.allowAllMessagesToMember(follower.getLocalMember(), leader.getLeader());
            }
        }

        f1.get();
        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : followers) {
                if (follower != slowFollower) {
                    assertEquals(6, getCommittedGroupMembers(follower).memberCount());
                } else {
                    assertEquals(5, getCommittedGroupMembers(follower).memberCount());
                    assertEquals(6, getLastGroupMembers(follower).memberCount());
                }
            }
        });

        RaftNodeImpl newRaftNode2 = group.createNewRaftNode();
        leader.replicateMembershipChange(newRaftNode2.getLocalMember(), MembershipChangeMode.ADD).get();

        group.allowAllMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember());
        group.allowAllMessagesToMember(slowFollower.getLocalMember(), leader.getLocalMember());
        group.allowAllMessagesToMember(leader.getLocalMember(), newRaftNode1.getLocalMember());

        RaftGroupMembers leaderCommittedGroupMembers = getCommittedGroupMembers(leader);
        assertTrueEventually(() -> {
            assertEquals(leaderCommittedGroupMembers.index(), getCommittedGroupMembers(slowFollower).index());
            assertEquals(leaderCommittedGroupMembers.index(), getCommittedGroupMembers(newRaftNode1).index());
            assertEquals(leaderCommittedGroupMembers.index(), getCommittedGroupMembers(newRaftNode2).index());
        });
    }

    @Test
    public void when_leaderIsSteppingDown_then_itDoesNotAcceptNewAppends() throws ExecutionException, InterruptedException {
        group = new LocalRaftGroupBuilder(3).setAppendNopEntryOnLeaderElection(true).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        group.dropMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);

        InternalCompletableFuture f1 = leader.replicateMembershipChange(leader.getLocalMember(), REMOVE);
        InternalCompletableFuture f2 = leader.replicate(new PostponedResponseRaftRunnable());

        assertFalse(f1.isDone());
        assertTrueEventually(() -> assertTrue(f2.isDone()));

        try {
            f2.joinInternal();
            fail();
        } catch (CannotReplicateException ignored) {
        }
    }

    @Test
    public void when_replicatedMembershipChangeIsReverted_then_itCanBeCommittedOnSecondReplicate() throws ExecutionException, InterruptedException {
        group = new LocalRaftGroupBuilder(3).setAppendNopEntryOnLeaderElection(true).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        leader.replicate(new ApplyRaftRunnable("val1")).get();
        long oldLeaderCommitIndexBeforeMemberhipChange = getCommitIndex(leader);

        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertEquals(oldLeaderCommitIndexBeforeMemberhipChange, getCommitIndex(follower));
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);

        RaftNodeImpl newRaftNode = group.createNewRaftNode();

        leader.replicateMembershipChange(newRaftNode.getLocalMember(), MembershipChangeMode.ADD);

        assertTrueEventually(() -> {
            long leaderLastLogIndex = getLastLogOrSnapshotEntry(leader).index();
            assertTrue(leaderLastLogIndex > oldLeaderCommitIndexBeforeMemberhipChange);
            assertEquals(leaderLastLogIndex, getLastLogOrSnapshotEntry(newRaftNode).index());
        });

        group.dropMessagesToAll(newRaftNode.getLocalMember(), PreVoteRequest.class);
        group.dropMessagesToAll(newRaftNode.getLocalMember(), VoteRequest.class);

        group.terminateNode(leader.getLocalMember());

        assertTrueEventually(() -> {
            assertNotNull(followers[0].getLeader());
            assertNotNull(followers[1].getLeader());
            assertNotEquals(leader.getLocalMember(), followers[0].getLeader());
            assertNotEquals(leader.getLocalMember(), followers[1].getLeader());
            assertEquals(followers[0].getLeader(), followers[1].getLeader());
        });

        RaftNodeImpl newLeader = group.getNode(followers[0].getLeader());
        newLeader.replicate(new ApplyRaftRunnable("val1")).get();
        newLeader.replicateMembershipChange(newRaftNode.getLocalMember(), MembershipChangeMode.ADD).get();

        assertTrueEventually(() -> {
            assertEquals(getCommitIndex(newLeader), getCommitIndex(newRaftNode));
            assertEquals(getCommittedGroupMembers(newLeader).index(), getCommittedGroupMembers(newRaftNode).index());
        });
    }

    static class PostponedResponseRaftRunnable implements RaftRunnable {
        @Override
        public Object run(Object service, long commitIndex) {
            return PostponedResponse.INSTANCE;
        }
    }

}
