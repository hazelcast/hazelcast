package com.hazelcast.raft.impl;

import com.hazelcast.config.raft.RaftAlgorithmConfig;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.MemberAlreadyExistsException;
import com.hazelcast.raft.exception.MemberDoesNotExistException;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.service.ApplyRaftRunnable;
import com.hazelcast.raft.impl.service.RaftDataService;
import com.hazelcast.raft.impl.state.RaftGroupMembers;
import com.hazelcast.raft.impl.testing.LocalRaftGroup;
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

import static com.hazelcast.raft.MembershipChangeType.REMOVE;
import static com.hazelcast.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.RaftUtil.getCommittedGroupMembers;
import static com.hazelcast.raft.impl.RaftUtil.getLastGroupMembers;
import static com.hazelcast.raft.impl.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.impl.RaftUtil.getStatus;
import static com.hazelcast.raft.impl.RaftUtil.newGroupWithService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MembershipChangeTest extends HazelcastTestSupport {

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
    public void when_newRaftNodeJoins_then_itAppendsMissingEntries() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new ApplyRaftRunnable("val")).get();

        final RaftNodeImpl newRaftNode = group.createNewRaftNode();

        leader.replicateMembershipChange(newRaftNode.getLocalMember(), MembershipChangeType.ADD).get();

        final long commitIndex = getCommitIndex(leader);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(commitIndex, getCommitIndex(newRaftNode));
            }
        });

        final RaftGroupMembers lastGroupMembers = RaftUtil.getLastGroupMembers(leader);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftNodeImpl[] nodes = group.getNodes();
                for (RaftNodeImpl raftNode : nodes) {
                    assertEquals(RaftNodeStatus.ACTIVE, getStatus(raftNode));
                    assertEquals(lastGroupMembers.members(), getLastGroupMembers(raftNode).members());
                    assertEquals(lastGroupMembers.index(), getLastGroupMembers(raftNode).index());
                    assertEquals(lastGroupMembers.members(), getCommittedGroupMembers(raftNode).members());
                    assertEquals(lastGroupMembers.index(), getCommittedGroupMembers(raftNode).index());
                }
            }
        });

        RaftDataService service = group.getService(newRaftNode);
        assertEquals(1, service.size());
        assertTrue(service.values().contains("val"));
    }

    @Test
    public void when_followerLeaves_then_itIsRemovedFromTheGroupMembers() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        final RaftNodeImpl leavingFollower = followers[0];
        final RaftNodeImpl stayingFollower = followers[1];

        leader.replicate(new ApplyRaftRunnable("val")).get();

        leader.replicateMembershipChange(leavingFollower.getLocalMember(), REMOVE).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : asList(leader, stayingFollower)) {
                    assertFalse(getLastGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalMember()));
                    assertFalse(getCommittedGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalMember()));
                }
            }
        });

        group.terminateNode(leavingFollower.getLocalMember());
    }

    @Test
    public void when_newRaftNodeJoinsAfterAnotherNodeLeaves_then_itAppendsMissingEntries() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new ApplyRaftRunnable("val")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        final RaftNodeImpl leavingFollower = followers[0];
        final RaftNodeImpl stayingFollower = followers[1];

        leader.replicateMembershipChange(leavingFollower.getLocalMember(), REMOVE).get();

        final RaftNodeImpl newRaftNode = group.createNewRaftNode();

        leader.replicateMembershipChange(newRaftNode.getLocalMember(), MembershipChangeType.ADD).get();

        final long commitIndex = getCommitIndex(leader);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(commitIndex, getCommitIndex(newRaftNode));
            }
        });

        final RaftGroupMembers lastGroupMembers = RaftUtil.getLastGroupMembers(leader);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : asList(leader, stayingFollower, newRaftNode)) {
                    assertEquals(RaftNodeStatus.ACTIVE, getStatus(raftNode));
                    assertEquals(lastGroupMembers.members(), getLastGroupMembers(raftNode).members());
                    assertEquals(lastGroupMembers.index(), getLastGroupMembers(raftNode).index());
                    assertEquals(lastGroupMembers.members(), getCommittedGroupMembers(raftNode).members());
                    assertEquals(lastGroupMembers.index(), getCommittedGroupMembers(raftNode).index());
                    assertFalse(getLastGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalMember()));
                    assertFalse(getCommittedGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalMember()));
                }
            }
        });

        RaftDataService service = group.getService(newRaftNode);
        assertEquals(1, service.size());
        assertTrue(service.values().contains("val"));
    }

    @Test
    public void when_newRaftNodeJoinsAfterAnotherNodeLeavesAndSnapshotIsTaken_then_itAppendsMissingEntries() throws ExecutionException, InterruptedException {
        int commitIndexAdvanceCountToSnapshot = 10;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig();
        config.setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCountToSnapshot);
        group = newGroupWithService(3, config);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new ApplyRaftRunnable("val")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        final RaftNodeImpl leavingFollower = followers[0];
        final RaftNodeImpl stayingFollower = followers[1];

        leader.replicateMembershipChange(leavingFollower.getLocalMember(), REMOVE).get();

        for (int i = 0; i < commitIndexAdvanceCountToSnapshot; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getSnapshotEntry(leader).index() > 0);
            }
        });

        final RaftNodeImpl newRaftNode = group.createNewRaftNode();

        leader.replicateMembershipChange(newRaftNode.getLocalMember(), MembershipChangeType.ADD).get();

        final long commitIndex = getCommitIndex(leader);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(commitIndex, getCommitIndex(newRaftNode));
            }
        });

        final RaftGroupMembers lastGroupMembers = RaftUtil.getLastGroupMembers(leader);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : asList(leader, stayingFollower, newRaftNode)) {
                    assertEquals(RaftNodeStatus.ACTIVE, getStatus(raftNode));
                    assertEquals(lastGroupMembers.members(), getLastGroupMembers(raftNode).members());
                    assertEquals(lastGroupMembers.index(), getLastGroupMembers(raftNode).index());
                    assertEquals(lastGroupMembers.members(), getCommittedGroupMembers(raftNode).members());
                    assertEquals(lastGroupMembers.index(), getCommittedGroupMembers(raftNode).index());
                    assertFalse(getLastGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalMember()));
                    assertFalse(getCommittedGroupMembers(raftNode).isKnownMember(leavingFollower.getLocalMember()));
                }
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
        group = newGroupWithService(3, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(leader.getLocalMember(), REMOVE).get();

        assertEquals(RaftNodeStatus.STEPPED_DOWN, getStatus(leader));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : followers) {
                    assertFalse(getLastGroupMembers(raftNode).isKnownMember(leader.getLocalMember()));
                    assertFalse(getCommittedGroupMembers(raftNode).isKnownMember(leader.getLocalMember()));
                }
            }
        });
    }

    @Test
    public void when_leaderLeaves_then_itCannotVoteForCommitOfMemberChange() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(1000));
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        group.dropMessagesToMember(followers[0].getLocalMember(), leader.getLocalMember(), AppendSuccessResponse.class);
        leader.replicate(new ApplyRaftRunnable("val")).get();

        leader.replicateMembershipChange(leader.getLocalMember(), MembershipChangeType.REMOVE);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, getCommitIndex(leader));
            }
        }, 10);
    }

    @Test
    public void when_leaderLeaves_then_followersElectNewLeader() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(leader.getLocalMember(), REMOVE).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : followers) {
                    assertFalse(getLastGroupMembers(raftNode).isKnownMember(leader.getLocalMember()));
                    assertFalse(getCommittedGroupMembers(raftNode).isKnownMember(leader.getLocalMember()));
                }
            }
        });

        group.terminateNode(leader.getLocalMember());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl raftNode : followers) {
                    assertNotEquals(leader.getLocalMember(), getLeaderMember(raftNode));
                }
            }
        });
    }

    @Test
    public void when_membershipChangeRequestIsMadeWithWrongType_then_theChangeFails() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        leader.replicate(new ApplyRaftRunnable("val")).get();

        try {
            leader.replicateMembershipChange(leader.getLocalMember(), null).get();
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void when_nonExistingEndpointIsRemoved_then_theChangeFails() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl leavingFollower = group.getAnyFollowerNode();

        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(leavingFollower.getLocalMember(), MembershipChangeType.REMOVE).get();

        try {
            leader.replicateMembershipChange(leavingFollower.getLocalMember(), MembershipChangeType.REMOVE).get();
            fail();
        } catch (MemberDoesNotExistException ignored) {
        }
    }

    @Test
    public void when_existingEndpointIsAdded_then_theChangeFails() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new ApplyRaftRunnable("val")).get();

        try {
            leader.replicateMembershipChange(leader.getLocalMember(), MembershipChangeType.ADD).get();
            fail();
        } catch (MemberAlreadyExistsException ignored) {
        }
    }

    @Test
    public void when_thereIsNoCommitInTheCurrentTerm_then_cannotMakeMemberChange() throws ExecutionException, InterruptedException {
        // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

        group = newGroupWithService(3, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        try {
            leader.replicateMembershipChange(leader.getLocalMember(), MembershipChangeType.REMOVE).get();
            fail();
        } catch (CannotReplicateException ignored) {
        }
    }

    @Test
    public void when_appendNopEntryOnLeaderElection_then_canMakeMemberChangeAfterNopEntryCommitted() {
        // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

        group = newGroupWithService(3, new RaftAlgorithmConfig(), true);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                // may fail until nop-entry is committed
                try {
                    leader.replicateMembershipChange(leader.getLocalMember(), MembershipChangeType.REMOVE).get();
                } catch (CannotReplicateException e) {
                    fail(e.getMessage());
                }
            }
        });
    }

    @Test
    public void when_newJoiningNodeFirstReceivesSnapshot_then_itInstallsSnapshot() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(5));
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        for (int i = 0; i < 4; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        final RaftNodeImpl newRaftNode = group.createNewRaftNode();

        group.dropMessagesToMember(leader.getLocalMember(), newRaftNode.getLocalMember(), AppendRequest.class);

        leader.replicateMembershipChange(newRaftNode.getLocalMember(), MembershipChangeType.ADD).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getSnapshotEntry(leader).index() > 0);
            }
        });

        group.resetAllDropRulesFrom(leader.getLocalMember());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(getCommitIndex(leader), getCommitIndex(newRaftNode));
                assertEquals(getLastGroupMembers(leader).members(), getLastGroupMembers(newRaftNode).members());
                assertEquals(getLastGroupMembers(leader).members(), getCommittedGroupMembers(newRaftNode).members());
                RaftDataService service = group.getService(newRaftNode);
                assertEquals(4, service.size());
            }
        });
    }

    @Test
    public void when_leaderFailsWhileLeavingRaftGroup_othersCommitTheMemberChange() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        leader.replicate(new ApplyRaftRunnable("val")).get();

        for (RaftNodeImpl follower : followers) {
            group.dropMessagesToMember(follower.getLocalMember(), leader.getLocalMember(), AppendSuccessResponse.class);
            group.dropMessagesToMember(follower.getLocalMember(), leader.getLocalMember(), AppendFailureResponse.class);
        }

        leader.replicateMembershipChange(leader.getLocalMember(), MembershipChangeType.REMOVE);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl follower : followers) {
                    assertEquals(2, getLastLogOrSnapshotEntry(follower).index());
                }
            }
        });

        group.terminateNode(leader.getLocalMember());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl follower : followers) {
                    final RaftMember newLeaderEndpoint = getLeaderMember(follower);
                    assertNotNull(newLeaderEndpoint);
                    assertNotEquals(leader.getLocalMember(), newLeaderEndpoint);
                }
            }
        });

        final RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));
        newLeader.replicate(new ApplyRaftRunnable("val2"));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (RaftNodeImpl follower : followers) {
                    assertFalse(getCommittedGroupMembers(follower).isKnownMember(leader.getLocalMember()));
                }
            }
        });
    }
}
