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
import com.hazelcast.cp.exception.StaleAppendRequestException;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.impl.command.UpdateRaftGroupMembersCmd;
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dataservice.RaftDataService;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.ACTIVE;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommittedGroupMembers;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getMatchIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getStatus;
import static com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder.newGroup;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotTest extends HazelcastTestSupport {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void when_commitLogAdvances_then_snapshotIsTaken() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(entryCount, getCommitIndex(raftNode));
                assertEquals(entryCount, getSnapshotEntry(raftNode).index());
                RaftDataService service = group.getService(raftNode);
                assertEquals(entryCount, service.size());
                for (int i = 0; i < entryCount; i++) {
                    assertEquals(("val" + i), service.get(i + 1));
                }
            }
        });
    }

    @Test
    public void when_snapshotIsTaken_then_nextEntryIsCommitted() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(entryCount, getCommitIndex(raftNode));
                assertEquals(entryCount, getSnapshotEntry(raftNode).index());
            }
        });

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(entryCount + 1, getCommitIndex(raftNode));
                RaftDataService service = group.getService(raftNode);
                assertEquals(entryCount + 1, service.size());
                for (int i = 0; i < entryCount; i++) {
                    assertEquals(("val" + i), service.get(i + 1));
                }
                assertEquals("valFinal", service.get(51));
            }
        });
    }

    @Test
    public void when_followersMatchIndexIsUnknown_then_itInstallsSnapshot() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl slowFollower = followers[1];

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), InstallSnapshot.class);

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> assertEquals(entryCount, getSnapshotEntry(leader).index()));

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        group.allowMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), InstallSnapshot.class);

        assertTrueEventually(() -> assertEquals(entryCount, getCommitIndex(slowFollower)));

        group.resetAllRulesFrom(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(entryCount + 1, getCommitIndex(raftNode));
                RaftDataService service = group.getService(raftNode);
                assertEquals(entryCount + 1, service.size());
                for (int i = 0; i < entryCount; i++) {
                    assertEquals(("val" + i), service.get(i + 1));
                }
                assertEquals("valFinal", service.get(51));
            }
        });
    }

    @Test
    public void when_followersIsFarBehind_then_itInstallsSnapshot() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new ApplyRaftRunnable("val0")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl slowFollower = followers[1];

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), InstallSnapshot.class);

        for (int i = 1; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> assertEquals(entryCount, getSnapshotEntry(leader).index()));

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        group.allowMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), InstallSnapshot.class);

        assertTrueEventually(() -> assertEquals(entryCount, getCommitIndex(slowFollower)));

        group.resetAllRulesFrom(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(entryCount + 1, getCommitIndex(raftNode));
                RaftDataService service = group.getService(raftNode);
                assertEquals(entryCount + 1, service.size());
                for (int i = 0; i < entryCount; i++) {
                    assertEquals(("val" + i), service.get(i + 1));
                }
                assertEquals("valFinal", service.get(51));
            }
        });
    }

    @Test
    public void when_leaderMissesInstallSnapshotResponse_then_itAdvancesMatchIndexWithNextInstallSnapshotResponse()
            throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount)
                                                              .setAppendRequestBackoffTimeoutInMillis(1000);
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl slowFollower = followers[1];

        // the leader cannot send AppendEntriesRPC to the follower
        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);

        // the follower cannot send append response to the leader after installing the snapshot
        group.dropMessagesToMember(slowFollower.getLocalMember(), leader.getLocalMember(), AppendSuccessResponse.class);

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> assertEquals(entryCount, getSnapshotEntry(leader).index()));

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        group.resetAllRulesFrom(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodesExcept(slowFollower.getLocalMember())) {
                assertEquals(entryCount + 1, getCommitIndex(raftNode));
                RaftDataService service = group.getService(raftNode);
                assertEquals(entryCount + 1, service.size());
                for (int i = 0; i < entryCount; i++) {
                    assertEquals(("val" + i), service.get(i + 1));
                }
                assertEquals("valFinal", service.get(51));
            }

            assertEquals(entryCount, getCommitIndex(slowFollower));
            RaftDataService service = group.getService(slowFollower);
            assertEquals(entryCount, service.size());
            for (int i = 0; i < entryCount; i++) {
                assertEquals(("val" + i), service.get(i + 1));
            }
        });

        group.resetAllRulesFrom(slowFollower.getLocalMember());

        long commitIndex = getCommitIndex(leader);

        assertTrueEventually(() -> {
            for (RaftNode raftNode : group.getNodesExcept(leader.getLocalMember())) {
                assertEquals(commitIndex, getMatchIndex(leader, raftNode.getLocalMember()));
            }
        });
    }

    @Test
    public void when_followerMissesTheLastEntryThatGoesIntoTheSnapshot_then_itCatchesUpWithoutInstallingSnapshot() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl slowFollower = followers[1];

        for (int i = 0; i < entryCount - 1; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : group.getNodesExcept(leader.getLocalMember())) {
                assertEquals(entryCount - 1, getMatchIndex(leader, follower.getLocalMember()));
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), InstallSnapshot.class);

        leader.replicate(new ApplyRaftRunnable("val" + (entryCount - 1))).get();

        assertTrueEventually(() -> assertEquals(entryCount, getSnapshotEntry(leader).index()));

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        group.allowMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(entryCount + 1, getCommitIndex(raftNode));
                RaftDataService service = group.getService(raftNode);
                assertEquals(entryCount + 1, service.size());
                for (int i = 0; i < entryCount; i++) {
                    assertEquals(("val" + i), service.get(i + 1));
                }
                assertEquals("valFinal", service.get(51));
            }
        });
    }

    @Test
    public void when_followerMissesAFewEntriesBeforeTheSnapshot_then_itCatchesUpWithoutInstallingSnapshot() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        final int missingEntryCountOnSlowFollower = 4; // entryCount * 0.1 - 2
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl slowFollower = followers[1];

        for (int i = 0; i < entryCount - missingEntryCountOnSlowFollower; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : group.getNodesExcept(leader.getLocalMember())) {
                assertEquals(entryCount - missingEntryCountOnSlowFollower, getMatchIndex(leader, follower.getLocalMember()));
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), InstallSnapshot.class);

        for (int i = entryCount - missingEntryCountOnSlowFollower; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> assertEquals(entryCount, getSnapshotEntry(leader).index()));

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        group.allowMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(entryCount + 1, getCommitIndex(raftNode));
                RaftDataService service = group.getService(raftNode);
                assertEquals(entryCount + 1, service.size());
                for (int i = 0; i < entryCount; i++) {
                    assertEquals(("val" + i), service.get(i + 1));
                }
                assertEquals("valFinal", service.get(51));
            }
        });
    }

    @Test
    public void when_isolatedLeaderAppendsEntries_then_itInvalidatesTheirFeaturesUponInstallSnapshot() throws ExecutionException, InterruptedException {
        int entryCount = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        for (int i = 0; i < 40; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(40, getCommitIndex(raftNode));
            }
        });

        group.split(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint leaderEndpoint = getLeaderMember(raftNode);
                assertNotNull(leaderEndpoint);
                assertNotEquals(leader.getLocalMember(), leaderEndpoint);
            }
        });

        List<InternalCompletableFuture> futures = new ArrayList<>();
        for (int i = 40; i < 45; i++) {
            InternalCompletableFuture f = leader.replicate(new ApplyRaftRunnable("isolated" + i));
            futures.add(f);
        }

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));

        for (int i = 40; i < 51; i++) {
            newLeader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertTrue(getSnapshotEntry(raftNode).index() > 0);
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);
        group.merge();

        for (InternalCompletableFuture f : futures) {
            try {
                f.joinInternal();
                fail();
            } catch (StaleAppendRequestException ignored) {
            }
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(51, getCommitIndex(raftNode));
                RaftDataService service = group.getService(raftNode);
                assertEquals(51, service.size());
                for (int i = 0; i < 51; i++) {
                    assertEquals(("val" + i), service.get(i + 1));
                }

            }
        });
    }

    @Test
    public void when_followersLastAppendIsMembershipChange_then_itUpdatesRaftNodeStateWithInstalledSnapshot() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        final RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
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
        Future f1 = leader.replicateMembershipChange(newRaftNode1.getLocalMember(), MembershipChangeMode.ADD);

        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertEquals(2L, getLastLogOrSnapshotEntry(follower).index());
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), InstallSnapshot.class);

        for (RaftNodeImpl follower : followers) {
            if (follower != slowFollower) {
                group.allowAllMessagesToMember(follower.getLocalMember(), leader.getLeader());
            }
        }

        f1.get();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> assertThat(getSnapshotEntry(leader).index(), greaterThanOrEqualTo((long) entryCount)));

        group.allowAllMessagesToMember(leader.getLeader(), slowFollower.getLocalMember());

        assertTrueEventually(() -> assertThat(getSnapshotEntry(slowFollower).index(), greaterThanOrEqualTo((long) entryCount)));

        assertTrueEventually(() -> {
            assertEquals(getCommittedGroupMembers(leader).index(), getCommittedGroupMembers(slowFollower).index());
            assertEquals(ACTIVE, getStatus(slowFollower));
        });
    }

    @Test
    public void testMembershipChangeBlocksSnapshotBug() throws ExecutionException, InterruptedException {
        // The comments below show how the code behaves before the mentioned bug is fixed.

        int commitIndexAdvanceCount = 50;
        int uncommittedEntryCount = 10;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCount)
                .setUncommittedEntryCountToRejectNewAppends(uncommittedEntryCount);
        group = newGroup(3, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        group.dropMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember(), AppendRequest.class);

        while (getSnapshotEntry(leader).index() == 0) {
            leader.replicate(new ApplyRaftRunnable("into_snapshot")).get();
        }

        // now, the leader has taken a snapshot.
        // It also keeps some already committed entries in the log because followers[0] hasn't appended them.
        // LOG: [ <46 - 49>, <50>], SNAPSHOT INDEX: 50, COMMIT INDEX: 50

        long leaderCommitIndex = getCommitIndex(leader);
        do {
            leader.replicate(new ApplyRaftRunnable("committed_after_snapshot")).get();
        } while (getCommitIndex(leader) < leaderCommitIndex + commitIndexAdvanceCount - 1);

        // committing new entries.
        // one more entry is needed to take the next snapshot.
        // LOG: [ <46 - 49>, <50>, <51 - 99 (committed)> ], SNAPSHOT INDEX: 50, COMMIT INDEX: 99

        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);

        for (int i = 0; i < uncommittedEntryCount - 1; i++) {
            leader.replicate(new ApplyRaftRunnable("uncommitted_after_snapshot"));
        }

        // appended some more entries which will not be committed because the leader has no majority.
        // the last uncommitted index is reserved for membership changed.
        // LOG: [ <46 - 49>, <50>, <51 - 99 (committed)>, <100 - 108 (uncommitted)> ], SNAPSHOT INDEX: 50, COMMIT INDEX: 99
        // There are only 2 empty indices in the log.

        RaftNodeImpl newRaftNode = group.createNewRaftNode();

        Function<Object, Object> alterFunc = o -> {
            if (o instanceof AppendRequest) {
                AppendRequest request = (AppendRequest) o;
                LogEntry[] entries = request.entries();
                if (entries.length > 0) {
                    if (entries[entries.length - 1].operation() instanceof UpdateRaftGroupMembersCmd) {
                        entries = Arrays.copyOf(entries, entries.length - 1);
                        return new AppendRequest(request.leader(), request.term(), request.prevLogTerm(), request.prevLogIndex(), request.leaderCommitIndex(), entries, request.queryRound());
                    } else if (entries[0].operation() instanceof UpdateRaftGroupMembersCmd) {
                        entries = new LogEntry[0];
                        return new AppendRequest(request.leader(), request.term(), request.prevLogTerm(), request.prevLogIndex(), request.leaderCommitIndex(), entries, request.queryRound());
                    }
                }
            }

            return null;
        };

        group.alterMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), alterFunc);
        group.alterMessagesToMember(leader.getLocalMember(), newRaftNode.getLocalMember(), alterFunc);

        long lastLogIndex1 = getLastLogOrSnapshotEntry(leader).index();

        leader.replicateMembershipChange(newRaftNode.getLocalMember(), MembershipChangeMode.ADD);

        // When the membership change entry is appended, the leader's Log will be as following:
        // LOG: [ <46 - 49>, <50>, <51 - 99 (committed)>, <100 - 108 (uncommitted)>, <109 (membership change)> ], SNAPSHOT INDEX: 50, COMMIT INDEX: 99

        assertTrueEventually(() -> assertTrue(getLastLogOrSnapshotEntry(leader).index() > lastLogIndex1));

        group.allowMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);

        // Then, only the entries before the membership change will be committed because we alter the append request. The log will be:
        // LOG: [ <46 - 49>, <50>, <51 - 108 (committed)>, <109 (membership change)> ], SNAPSHOT INDEX: 50, COMMIT INDEX: 108
        // There is only 1 empty index in the log.

        assertTrueEventually(() -> {
            assertEquals(lastLogIndex1, getCommitIndex(leader));
            assertEquals(lastLogIndex1, getCommitIndex(followers[1]));
        });


//        assertTrueEventually(() -> {
//            assertEquals(lastLogIndex1 + 1, getCommitIndex(leader));
//            assertEquals(lastLogIndex1 + 1, getCommitIndex(followers[1]));
//        });

        long lastLogIndex2 = getLastLogOrSnapshotEntry(leader).index();

        leader.replicate(new ApplyRaftRunnable("after_membership_change_append"));

        assertTrueEventually(() -> assertTrue(getLastLogOrSnapshotEntry(leader).index() > lastLogIndex2));

        // Now the log is full. There is no empty space left.
        // LOG: [ <46 - 49>, <50>, <51 - 108 (committed)>, <109 (membership change)>, <110 (uncommitted)> ], SNAPSHOT INDEX: 50, COMMIT INDEX: 108

        long lastLogIndex3 = getLastLogOrSnapshotEntry(leader).index();

        Future f = leader.replicate(new ApplyRaftRunnable("after_membership_change_append"));

        assertTrueEventually(() -> assertTrue(getLastLogOrSnapshotEntry(leader).index() > lastLogIndex3));

        assertFalse(f.isDone());
    }

    @Test
    public void when_slowFollowerReceivesAppendRequestThatDoesNotFitIntoItsRaftLog_then_itTruncatesAppendRequestEntries()
            throws ExecutionException, InterruptedException {
        int appendRequestMaxEntryCount = 100;
        int commitIndexAdvanceCount = 100;
        int uncommittedEntryCount = 10;

        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setAppendRequestMaxEntryCount(appendRequestMaxEntryCount)
                .setCommitIndexAdvanceCountToSnapshot(commitIndexAdvanceCount)
                .setUncommittedEntryCountToRejectNewAppends(uncommittedEntryCount);
        group = newGroup(5, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl slowFollower1 = followers[0];
        RaftNodeImpl slowFollower2 = followers[1];

        int count = 1;
        for (int i = 0; i < commitIndexAdvanceCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + (count++))).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertTrue(getSnapshotEntry(node).index() > 0);
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower1.getLocalMember(), AppendRequest.class);

        for (int i = 0; i < commitIndexAdvanceCount - 1; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + (count++))).get();
        }

        assertTrueEventually(() -> assertEquals(getCommitIndex(leader), getCommitIndex(slowFollower2)));

        // slowFollower2's log: [ <91 - 100 before snapshot>, <100 snapshot>, <101 - 199 committed> ]

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower2.getLocalMember(), AppendRequest.class);

        for (int i = 0; i < commitIndexAdvanceCount / 2; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + (count++))).get();
        }

        assertTrueEventually(() -> assertTrue(getSnapshotEntry(leader).index() > commitIndexAdvanceCount));

        // leader's log: [ <191 - 199 before snapshot>, <200 snapshot>, <201 - 249 committed> ]

        group.allowMessagesToMember(leader.getLocalMember(), slowFollower2.getLocalMember(), AppendRequest.class);

        // leader replicates 50 entries to slowFollower2 but slowFollower2 has only available capacity of 11 indices.
        // so, slowFollower2 appends 11 of these 50 entries in the first AppendRequest, takes a snapshot,
        // and receives another AppendRequest for the remaining entries...

        assertTrueEventually(() -> {
            assertEquals(getCommitIndex(leader), getCommitIndex(slowFollower2));
            assertTrue(getSnapshotEntry(slowFollower2).index() > commitIndexAdvanceCount);
        });
    }
}
