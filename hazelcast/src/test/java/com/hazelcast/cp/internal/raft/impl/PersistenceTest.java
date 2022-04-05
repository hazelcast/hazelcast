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
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dataservice.RaftDataService;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.cp.internal.raft.impl.testing.InMemoryRaftStateStore;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder;
import com.hazelcast.function.BiFunctionEx;
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cp.internal.raft.MembershipChangeMode.REMOVE;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommittedGroupMembers;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastApplied;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastGroupMembers;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getRaftStateStore;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getRestoredState;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getTerm;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PersistenceTest extends HazelcastTestSupport {

    private static final BiFunctionEx<RaftEndpoint, RaftAlgorithmConfig, RaftStateStore> RAFT_STATE_STORE_FACTORY
            = (BiFunctionEx<RaftEndpoint, RaftAlgorithmConfig, RaftStateStore>) (endpoint, config) -> {
                int maxUncommittedEntryCount = config.getUncommittedEntryCountToRejectNewAppends();
                int commitIndexAdvanceCountToSnapshot = config.getCommitIndexAdvanceCountToSnapshot();
                int maxNumberOfLogsToKeepAfterSnapshot = (int) (commitIndexAdvanceCountToSnapshot * 0.5);
                int logCapacity = commitIndexAdvanceCountToSnapshot + maxUncommittedEntryCount + maxNumberOfLogsToKeepAfterSnapshot;
                return new InMemoryRaftStateStore(logCapacity);
            };

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void testTermAndVoteArePersisted() {
        group = new LocalRaftGroupBuilder(3).setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY).build();
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        final Set<RaftEndpoint> endpoints = new HashSet<RaftEndpoint>();
        for (RaftNodeImpl node : group.getNodes()) {
            endpoints.add(node.getLocalMember());
        }

        final int term1 = getTerm(leader);
        assertTrueEventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                RestoredRaftState restoredState = getRestoredState(node);
                assertEquals(node.getLocalMember(), restoredState.localEndpoint());
                assertEquals(term1, restoredState.term());
                assertEquals(endpoints, restoredState.initialMembers());

            }
        });

        group.terminateNode(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : followers) {
                RaftEndpoint l = node.getLeader();
                assertNotNull(l);
                assertNotEquals(leader.getLeader(), l);
            }
        });

        final RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        final int term2 = getTerm(newLeader);

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : followers) {
                RestoredRaftState restoredState = getRestoredState(node);
                assertEquals(term2, restoredState.term());
                assertEquals(newLeader.getLocalMember(), restoredState.votedFor());
            }
        });
    }

    @Test
    public void testCommittedEntriesArePersisted() throws ExecutionException, InterruptedException {
        group = new LocalRaftGroupBuilder(3).setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        final int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                RestoredRaftState restoredState = getRestoredState(node);
                LogEntry[] entries = restoredState.entries();
                assertEquals(count, entries.length);
                for (int i = 0; i < count; i++) {
                    LogEntry entry = entries[i];
                    assertEquals(i + 1, entry.index());
                    assertEquals("val" + i, ((ApplyRaftRunnable) entry.operation()).getVal());
                }
            }
        });
    }

    @Test
    public void testUncommittedEntriesArePersisted() {
        group = new LocalRaftGroupBuilder(3).setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY).build();
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        final RaftNodeImpl responsiveFollower = followers[0];

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToMember(leader.getLocalMember(), followers[i].getLocalMember(), AppendRequest.class);
        }

        final int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i));
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : Arrays.asList(leader, responsiveFollower)) {
                RestoredRaftState restoredState = getRestoredState(node);
                LogEntry[] entries = restoredState.entries();
                assertEquals(count, entries.length);
                for (int i = 0; i < count; i++) {
                    LogEntry entry = entries[i];
                    assertEquals(i + 1, entry.index());
                    assertEquals("val" + i, ((ApplyRaftRunnable) entry.operation()).getVal());
                }
            }
        });
    }

    @Test
    public void testSnapshotIsPersisted() throws ExecutionException, InterruptedException {
        final int committedEntryCountToSnapshot = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot);
        group = new LocalRaftGroupBuilder(3, config).setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY).build();
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        for (int i = 0; i < committedEntryCountToSnapshot; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> {
            assertEquals(committedEntryCountToSnapshot, getSnapshotEntry(leader).index());

            for (RaftNodeImpl node : group.getNodes()) {
                RestoredRaftState restoredState = getRestoredState(node);
                SnapshotEntry snapshot = restoredState.snapshot();
                assertNotNull(snapshot);
                assertEquals(committedEntryCountToSnapshot, snapshot.index());
            }
        });
    }

    @Test
    public void when_leaderAppendEntriesInMinoritySplit_then_itTruncatesEntriesOnStore() throws ExecutionException, InterruptedException {
        group = new LocalRaftGroupBuilder(3).setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY).build();
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new ApplyRaftRunnable("val1")).get();

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(1, getCommitIndex(raftNode));
            }
        });

        final RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        group.split(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint leaderEndpoint = getLeaderMember(raftNode);
                assertNotNull(leaderEndpoint);
                assertNotEquals(leader.getLocalMember(), leaderEndpoint);
            }
        });

        for (int i = 0; i < 10; i++) {
            leader.replicate(new ApplyRaftRunnable("isolated" + i));
        }

        assertTrueEventually(() -> {
            RestoredRaftState restoredState = getRestoredState(leader);
            LogEntry[] entries = restoredState.entries();
            assertEquals(11, entries.length);
            assertEquals("val1", ((ApplyRaftRunnable) entries[0].operation()).getVal());
            for (int i = 1; i < 11; i++) {
                assertEquals("isolated" + (i - 1), ((ApplyRaftRunnable) entries[i].operation()).getVal());
            }
        });

        RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));
        for (int i = 0; i < 10; i++) {
            newLeader.replicate(new ApplyRaftRunnable("valNew" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertEquals(11, getCommitIndex(raftNode));
            }
        });

        group.merge();

        RaftNodeImpl finalLeader = group.waitUntilLeaderElected();

        assertNotEquals(leader.getLocalMember(), finalLeader.getLocalMember());

        assertTrueEventually(() -> {
            RestoredRaftState state = getRestoredState(leader);
            LogEntry[] entries = state.entries();
            assertEquals(11, entries.length);
            assertEquals("val1", ((ApplyRaftRunnable) entries[0].operation()).getVal());
            for (int i = 1; i < 11; i++) {
                assertEquals("valNew" + (i - 1), ((ApplyRaftRunnable) entries[i].operation()).getVal());
            }
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itBecomesFollowerAndRestoresItsRaftState() throws ExecutionException, InterruptedException {
        group = new LocalRaftGroupBuilder(3).setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY)
                                                    .setAppendNopEntryOnLeaderElection(true).build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        final int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        RaftEndpoint terminatedEndpoint = leader.getLocalMember();
        InMemoryRaftStateStore stateStore = getRaftStateStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        group.terminateNode(terminatedEndpoint);
        final RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        final RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertEquals(new ArrayList<>(getCommittedGroupMembers(newLeader).members()),
                new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
        assertEquals(new ArrayList<>(getLastGroupMembers(newLeader).members()),
                new ArrayList<>(getLastGroupMembers(restartedNode).members()));

        assertTrueEventually(() -> {
            assertEquals(newLeader.getLocalMember(), restartedNode.getLeader());
            assertEquals(getTerm(newLeader), getTerm(restartedNode));
            assertEquals(getCommitIndex(newLeader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(newLeader), getLastApplied(restartedNode));
            RaftDataService service = group.getService(restartedNode);
            Object[] values = service.valuesArray();
            assertThat(values, arrayWithSize(count));
            for (int i = 0; i < count; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itRestoresItsRaftStateAndBecomesLeader() throws ExecutionException, InterruptedException {
        group = new LocalRaftGroupBuilder(3).setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY)
                                            .setAppendNopEntryOnLeaderElection(true)
                                            .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        final int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        final int term = getTerm(leader);
        final long commitIndex = getCommitIndex(leader);

        RaftEndpoint terminatedEndpoint = leader.getLocalMember();
        InMemoryRaftStateStore stateStore = getRaftStateStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertSame(newLeader, restartedNode);

        assertTrueEventually(() -> {
            assertTrue(getTerm(restartedNode) > term);
            assertEquals(commitIndex + 1, getCommitIndex(restartedNode));
            RaftDataService service = group.getService(restartedNode);
            Object[] values = service.valuesArray();
            assertThat(values, arrayWithSize(count));
            for (int i = 0; i < count; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_followerIsRestarted_then_itRestoresItsRaftState() throws ExecutionException, InterruptedException {
        group = new LocalRaftGroupBuilder(3).setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY).build();
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl terminatedFollower = group.getAnyFollowerNode();
        final int count = 10;
        for (int i = 0; i < count; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> assertEquals(getCommitIndex(leader), getCommitIndex(terminatedFollower)));

        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalMember();
        InMemoryRaftStateStore stateStore = getRaftStateStore(terminatedFollower);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        group.terminateNode(terminatedEndpoint);
        leader.replicate(new ApplyRaftRunnable("val" + count)).get();

        final RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertEquals(new ArrayList<>(getCommittedGroupMembers(leader).members()),
                new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
        assertEquals(new ArrayList<>(getLastGroupMembers(leader).members()),
                new ArrayList<>(getLastGroupMembers(restartedNode).members()));

        assertTrueEventually(() -> {
            assertEquals(leader.getLocalMember(), restartedNode.getLeader());
            assertEquals(getTerm(leader), getTerm(restartedNode));
            assertEquals(getCommitIndex(leader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(leader), getLastApplied(restartedNode));
            RaftDataService service = group.getService(restartedNode);
            Object[] values = service.valuesArray();
            assertThat(values, arrayWithSize(count + 1));
            for (int i = 0; i <= count; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itBecomesFollowerAndRestoresItsRaftStateWithSnapshot() throws ExecutionException, InterruptedException {
        final int committedEntryCountToSnapshot = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot);
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY)
                                                    .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= committedEntryCountToSnapshot; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrue(getSnapshotEntry(leader).index() > 0);

        RaftEndpoint terminatedEndpoint = leader.getLocalMember();
        InMemoryRaftStateStore stateStore = getRaftStateStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        group.terminateNode(terminatedEndpoint);
        final RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        final RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertEquals(new ArrayList<>(getCommittedGroupMembers(newLeader).members()),
                new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
        assertEquals(new ArrayList<>(getLastGroupMembers(newLeader).members()),
                new ArrayList<>(getLastGroupMembers(restartedNode).members()));

        assertTrueEventually(() -> {
            assertEquals(newLeader.getLocalMember(), restartedNode.getLeader());
            assertEquals(getTerm(newLeader), getTerm(restartedNode));
            assertEquals(getCommitIndex(newLeader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(newLeader), getLastApplied(restartedNode));
            RaftDataService service = group.getService(restartedNode);
            Object[] values = service.valuesArray();
            assertThat(values, arrayWithSize(committedEntryCountToSnapshot + 1));
            for (int i = 0; i <= committedEntryCountToSnapshot; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_followerIsRestarted_then_itRestoresItsRaftStateWithSnapshot() throws ExecutionException, InterruptedException {
        final int committedEntryCountToSnapshot = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot);
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY)
                                                    .build();
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= committedEntryCountToSnapshot; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertTrue(getSnapshotEntry(node).index() > 0);
            }
        });

        RaftNodeImpl terminatedFollower = group.getAnyFollowerNode();
        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalMember();
        InMemoryRaftStateStore stateStore = getRaftStateStore(terminatedFollower);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        group.terminateNode(terminatedEndpoint);

        leader.replicate(new ApplyRaftRunnable("val" + (committedEntryCountToSnapshot + 1))).get();

        final RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertEquals(new ArrayList<>(getCommittedGroupMembers(leader).members()),
                new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
        assertEquals(new ArrayList<>(getLastGroupMembers(leader).members()),
                new ArrayList<>(getLastGroupMembers(restartedNode).members()));

        assertTrueEventually(() -> {
            assertEquals(leader.getLocalMember(), restartedNode.getLeader());
            assertEquals(getTerm(leader), getTerm(restartedNode));
            assertEquals(getCommitIndex(leader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(leader), getLastApplied(restartedNode));
            RaftDataService service = group.getService(restartedNode);
            Object[] values = service.valuesArray();
            assertThat(values, arrayWithSize(committedEntryCountToSnapshot + 2));
            for (int i = 0; i <= committedEntryCountToSnapshot + 1; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itRestoresItsRaftStateWithSnapshotAndBecomesLeader() throws ExecutionException, InterruptedException {
        final int committedEntryCountToSnapshot = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot);
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY)
                                                    .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i <= committedEntryCountToSnapshot; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrue(getSnapshotEntry(leader).index() > 0);
        final int term = getTerm(leader);
        final long commitIndex = getCommitIndex(leader);

        RaftEndpoint terminatedEndpoint = leader.getLocalMember();
        InMemoryRaftStateStore stateStore = getRaftStateStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertSame(restartedNode, newLeader);

        assertTrueEventually(() -> {
            assertTrue(getTerm(restartedNode) > term);
            assertEquals(commitIndex + 1, getCommitIndex(restartedNode));
            RaftDataService service = group.getService(restartedNode);
            Object[] values = service.valuesArray();
            assertThat(values, arrayWithSize(committedEntryCountToSnapshot + 1));
            for (int i = 0; i <= committedEntryCountToSnapshot; i++) {
                assertEquals("val" + i, values[i]);
            }
        });
    }

    private void blockVotingBetweenFollowers() {
        RaftEndpoint[] endpoints = group.getFollowerEndpoints();
        for (RaftEndpoint endpoint : endpoints) {
            if (group.isRunning(endpoint)) {
                group.dropMessagesToAll(endpoint, PreVoteRequest.class);
            }
        }
    }

    @Test
    public void when_leaderIsRestarted_then_itBecomesLeaderAndAppliesPreviouslyCommittedMemberList() throws ExecutionException, InterruptedException {
        group = new LocalRaftGroupBuilder(3).setAppendNopEntryOnLeaderElection(true)
                                            .setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY)
                                            .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl removedFollower = followers[0];
        RaftNodeImpl runningFollower = followers[1];

        group.terminateNode(removedFollower.getLocalMember());
        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(removedFollower.getLocalMember(), REMOVE).get();

        RaftEndpoint terminatedEndpoint = leader.getLocalMember();
        InMemoryRaftStateStore stateStore = getRaftStateStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertSame(restartedNode, newLeader);

        assertTrueEventually(() -> {
            assertEquals(getCommitIndex(runningFollower), getCommitIndex(restartedNode));
            assertEquals(new ArrayList<>(getCommittedGroupMembers(runningFollower).members()),
                    new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
            assertEquals(new ArrayList<>(getLastGroupMembers(runningFollower).members()),
                    new ArrayList<>(getLastGroupMembers(restartedNode).members()));
        });
    }

    @Test
    public void when_followerIsRestarted_then_itAppliesPreviouslyCommittedMemberList() throws ExecutionException, InterruptedException {
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(SECONDS.toMillis(30));
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY)
                                                    .build();
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl removedFollower = followers[0];
        RaftNodeImpl terminatedFollower = followers[1];

        group.terminateNode(removedFollower.getLocalMember());
        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(removedFollower.getLocalMember(), REMOVE).get();

        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalMember();
        InMemoryRaftStateStore stateStore = getRaftStateStore(terminatedFollower);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        group.terminateNode(terminatedEndpoint);
        final RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertTrueEventually(() -> {
            assertEquals(getCommitIndex(leader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(leader), getLastApplied(restartedNode));
            assertEquals(new ArrayList<>(getCommittedGroupMembers(leader).members()),
                    new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
            assertEquals(new ArrayList<>(getLastGroupMembers(leader).members()),
                    new ArrayList<>(getLastGroupMembers(restartedNode).members()));
        });
    }

    @Test
    public void when_leaderIsRestarted_then_itBecomesLeaderAndAppliesPreviouslyCommittedMemberListViaSnapshot() throws ExecutionException, InterruptedException {
        int committedEntryCountToSnapshot = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot);
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY)
                                                    .build();
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl removedFollower = followers[0];
        final RaftNodeImpl runningFollower = followers[1];

        group.terminateNode(removedFollower.getLocalMember());
        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(removedFollower.getLocalMember(), REMOVE).get();

        while (getSnapshotEntry(leader).index() == 0) {
            leader.replicate(new ApplyRaftRunnable("val")).get();
        }

        RaftEndpoint terminatedEndpoint = leader.getLocalMember();
        InMemoryRaftStateStore stateStore = getRaftStateStore(leader);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        // Block voting between followers
        // to avoid a leader election before leader restarts.
        blockVotingBetweenFollowers();

        group.terminateNode(terminatedEndpoint);
        RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertSame(restartedNode, newLeader);

        assertTrueEventually(() -> {
            assertEquals(getCommitIndex(runningFollower), getCommitIndex(restartedNode));
            assertEquals(new ArrayList<>(getCommittedGroupMembers(runningFollower).members()),
                    new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
            assertEquals(new ArrayList<>(getLastGroupMembers(runningFollower).members()),
                    new ArrayList<>(getLastGroupMembers(restartedNode).members()));
        });
    }

    @Test
    public void when_followerIsRestarted_then_itAppliesPreviouslyCommittedMemberListViaSnapshot() throws ExecutionException, InterruptedException {
        int committedEntryCountToSnapshot = 50;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(committedEntryCountToSnapshot)
                                                              .setLeaderHeartbeatPeriodInMillis(SECONDS.toMillis(30));
        group = new LocalRaftGroupBuilder(3, config).setAppendNopEntryOnLeaderElection(true)
                                                    .setRaftStateStoreFactory(RAFT_STATE_STORE_FACTORY)
                                                    .build();
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl removedFollower = followers[0];
        RaftNodeImpl terminatedFollower = followers[1];

        group.terminateNode(removedFollower.getLocalMember());
        leader.replicate(new ApplyRaftRunnable("val")).get();
        leader.replicateMembershipChange(removedFollower.getLocalMember(), REMOVE).get();

        while (getSnapshotEntry(terminatedFollower).index() == 0) {
            leader.replicate(new ApplyRaftRunnable("val")).get();
        }

        RaftEndpoint terminatedEndpoint = terminatedFollower.getLocalMember();
        InMemoryRaftStateStore stateStore = getRaftStateStore(terminatedFollower);
        RestoredRaftState terminatedState = stateStore.toRestoredRaftState();

        group.terminateNode(terminatedEndpoint);
        final RaftNodeImpl restartedNode = group.createNewRaftNode(terminatedState, stateStore);

        assertTrueEventually(() -> {
            assertEquals(getCommitIndex(leader), getCommitIndex(restartedNode));
            assertEquals(getLastApplied(leader), getLastApplied(restartedNode));
            assertEquals(new ArrayList<>(getCommittedGroupMembers(leader).members()),
                    new ArrayList<>(getCommittedGroupMembers(restartedNode).members()));
            assertEquals(new ArrayList<>(getLastGroupMembers(leader).members()),
                    new ArrayList<>(getLastGroupMembers(restartedNode).members()));
        });
    }

}
