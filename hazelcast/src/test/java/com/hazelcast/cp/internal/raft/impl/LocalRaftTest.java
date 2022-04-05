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
import com.hazelcast.cp.exception.StaleAppendRequestException;
import com.hazelcast.cp.internal.raft.impl.dataservice.ApplyRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dataservice.RaftDataService;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
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
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getRole;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getVotedFor;
import static com.hazelcast.cp.internal.raft.impl.testing.LocalRaftGroup.LocalRaftGroupBuilder.newGroup;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalRaftTest extends HazelcastTestSupport {

    private LocalRaftGroup group;

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void when_twoNodeCluster_then_leaderIsElected() {
        testLeaderElection(2);
    }

    @Test
    public void when_threeNodeCluster_then_leaderIsElected() {
        testLeaderElection(3);
    }

    private void testLeaderElection(int nodeCount) {
        group = newGroup(nodeCount);
        group.start();
        group.waitUntilLeaderElected();

        RaftEndpoint leaderEndpoint = group.getLeaderEndpoint();
        assertNotNull(leaderEndpoint);

        int leaderIndex = group.getLeaderIndex();
        assertThat(leaderIndex, greaterThanOrEqualTo(0));

        RaftNodeImpl leaderNode = group.getLeaderNode();
        assertNotNull(leaderNode);

        assertTrueEventually(() -> {
            for (RaftNodeImpl node : group.getNodes()) {
                assertEquals(leaderEndpoint, getLeaderMember(node));
            }
        });
    }

    @Test
    public void when_twoNodeCluster_then_singleEntryCommitted() throws Exception {
        testSingleCommitEntry(2);
    }

    @Test
    public void when_threeNodeCluster_then_singleEntryCommitted() throws Exception {
        testSingleCommitEntry(3);
    }

    private void testSingleCommitEntry(final int nodeCount) throws Exception {
        group = newGroup(nodeCount);
        group.start();
        group.waitUntilLeaderElected();

        Object val = "val";
        Object result = group.getLeaderNode().replicate(new ApplyRaftRunnable(val)).get();
        assertEquals(result, val);

        int commitIndex = 1;
        assertTrueEventually(() -> {
            for (int i = 0; i < nodeCount; i++) {
                RaftNodeImpl node = group.getNode(i);
                long index = getCommitIndex(node);
                assertEquals(commitIndex, index);
                RaftDataService service = group.getIntegration(i).getService();
                Object actual = service.get(commitIndex);
                assertEquals(val, actual);
            }
        });
    }

    @Test
    public void when_followerAttemptsToReplicate_then_itFails() throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        try {
            followers[0].replicate(new ApplyRaftRunnable("val")).joinInternal();
            fail("NotLeaderException should have been thrown");
        } catch (NotLeaderException e) {
            ignore(e);
        }

        for (RaftNodeImpl raftNode : group.getNodes()) {
            RaftDataService service = group.getIntegration(raftNode.getLocalMember()).getService();
            assertEquals(0, service.size());
        }
    }

    @Test
    public void when_twoNodeCluster_then_leaderCannotCommitWithOnlyLocalAppend() throws ExecutionException, InterruptedException {
        testNoCommitWhenOnlyLeaderAppends(2);
    }

    @Test
    public void when_threeNodeCluster_then_leaderCannotCommitWithOnlyLocalAppend() throws ExecutionException, InterruptedException {
        testNoCommitWhenOnlyLeaderAppends(3);
    }

    private void testNoCommitWhenOnlyLeaderAppends(int nodeCount) throws InterruptedException, ExecutionException {
        group = newGroup(nodeCount);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        group.dropMessagesToAll(leader.getLocalMember(), AppendRequest.class);

        try {
            leader.replicate(new ApplyRaftRunnable("val")).get(10, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }

        for (RaftNodeImpl raftNode : group.getNodes()) {
            assertEquals(0, getCommitIndex(raftNode));
            RaftDataService service = group.getIntegration(raftNode.getLocalMember()).getService();
            assertEquals(0, service.size());
        }
    }

    @Test
    public void when_leaderAppendsToMinority_then_itCannotCommit() throws ExecutionException, InterruptedException {
        group = newGroup(5);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToMember(leader.getLocalMember(), followers[i].getLocalMember(), AppendRequest.class);
        }

        Future f = leader.replicate(new ApplyRaftRunnable("val"));

        assertTrueEventually(() -> {
            assertEquals(1, getLastLogOrSnapshotEntry(leader).index());
            assertEquals(1, getLastLogOrSnapshotEntry(followers[0]).index());
        });

        try {
            f.get(10, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException ignored) {
        }

        for (RaftNodeImpl raftNode : group.getNodes()) {
            assertEquals(0, getCommitIndex(raftNode));
            RaftDataService service = group.getIntegration(raftNode.getLocalMember()).getService();
            assertEquals(0, service.size());
        }
    }

    @Test
    public void when_fourNodeCluster_then_leaderReplicateEntriesSequentially() throws ExecutionException, InterruptedException {
        testReplicateEntriesSequentially(4);
    }

    @Test
    public void when_fiveNodeCluster_then_leaderReplicateEntriesSequentially() throws ExecutionException, InterruptedException {
        testReplicateEntriesSequentially(5);
    }


    private void testReplicateEntriesSequentially(int nodeCount) throws ExecutionException, InterruptedException {
        final int entryCount = 100;
        group = newGroup(nodeCount, newRaftConfigWithNoSnapshotting(entryCount));
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            Object val = "val" + i;
            leader.replicate(new ApplyRaftRunnable(val)).get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(entryCount, getCommitIndex(raftNode));
                RaftDataService service = group.getService(raftNode);
                assertEquals(100, service.size());
                for (int i = 0; i < entryCount; i++) {
                    int commitIndex = i + 1;
                    Object val = "val" + i;
                    assertEquals(val, service.get(commitIndex));
                }
            }
        });
    }

    @Test
    public void when_fourNodeCluster_then_leaderReplicatesEntriesConcurrently() throws ExecutionException, InterruptedException {
        testReplicateEntriesConcurrently(4);
    }

    @Test
    public void when_fiveNodeCluster_then_leaderReplicatesEntriesConcurrently() throws ExecutionException, InterruptedException {
        testReplicateEntriesConcurrently(5);
    }

    private void testReplicateEntriesConcurrently(int nodeCount) throws ExecutionException, InterruptedException {
        final int entryCount = 100;
        group = newGroup(nodeCount, newRaftConfigWithNoSnapshotting(entryCount));
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        List<Future> futures = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            Object val = "val" + i;
            futures.add(leader.replicate(new ApplyRaftRunnable(val)));
        }

        for (Future f : futures) {
            f.get();
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(entryCount, getCommitIndex(raftNode));
                RaftDataService service = group.getService(raftNode);
                assertEquals(100, service.size());
                Set<Object> values = service.values();
                for (int i = 0; i < entryCount; i++) {
                    Object val = "val" + i;
                    assertTrue(values.contains(val));
                }
            }
        });
    }

    @Test
    public void when_fourNodeCluster_then_entriesAreSubmittedInParallel() throws InterruptedException {
        testReplicateEntriesInParallel(4);
    }

    @Test
    public void when_fiveNodeCluster_then_entriesAreSubmittedInParallel() throws InterruptedException {
        testReplicateEntriesInParallel(5);
    }

    private void testReplicateEntriesInParallel(int nodeCount) throws InterruptedException {
        int threadCount = 10;
        final int opsPerThread = 10;
        group = newGroup(nodeCount, newRaftConfigWithNoSnapshotting(threadCount * opsPerThread));
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int start = i * opsPerThread;
            threads[i] = new Thread(() -> {
                List<Future> futures = new ArrayList<>();
                for (int j = start; j < start + opsPerThread; j++) {
                    futures.add(leader.replicate(new ApplyRaftRunnable(j)));
                }

                for (Future f : futures) {
                    try {
                        f.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        int entryCount = threadCount * opsPerThread;

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(entryCount, getCommitIndex(raftNode));
                RaftDataService service = group.getService(raftNode);
                assertEquals(entryCount, service.size());
                Set<Object> values = service.values();
                for (int i = 0; i < entryCount; i++) {
                    assertTrue(values.contains(i));
                }
            }
        });
    }

    @Test
    public void when_followerSlowsDown_then_itCatchesLeaderEventually() throws ExecutionException, InterruptedException {
        final int entryCount = 100;
        group = newGroup(3, newRaftConfigWithNoSnapshotting(entryCount));
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl slowFollower = group.getAnyFollowerNode();

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);

        for (int i = 0; i < entryCount; i++) {
            Object val = "val" + i;
            leader.replicate(new ApplyRaftRunnable(val)).get();
        }

        assertEquals(0, getCommitIndex(slowFollower));

        group.resetAllRulesFrom(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(entryCount, getCommitIndex(raftNode));
                RaftDataService service = group.getService(raftNode);
                assertEquals(entryCount, service.size());
                Set<Object> values = service.values();
                for (int i = 0; i < entryCount; i++) {
                    Object val = "val" + i;
                    assertTrue(values.contains(val));
                }
            }
        });
    }

    @Test
    public void when_disruptiveFollowerStartsElection_then_itCannotTakeOverLeadershipFromLegitimateLeader()
            throws ExecutionException, InterruptedException {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leader = group.waitUntilLeaderElected();
        int leaderTerm = getTerm(leader);
        RaftNodeImpl disruptiveFollower = group.getAnyFollowerNode();

        group.dropMessagesToMember(leader.getLocalMember(), disruptiveFollower.getLocalMember(), AppendRequest.class);

        leader.replicate(new ApplyRaftRunnable("val")).get();

        group.split(disruptiveFollower.getLocalMember());

        int[] disruptiveFollowerTermRef = new int[1];
        assertTrueAllTheTime(() -> {
            int followerTerm = getTerm(disruptiveFollower);
            assertEquals(leaderTerm, followerTerm);
            disruptiveFollowerTermRef[0] = followerTerm;
        }, 5);

        group.resetAllRulesFrom(leader.getLocalMember());
        group.merge();

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertNotEquals(disruptiveFollower.getLocalMember(), newLeader.getLocalMember());
        assertEquals(getTerm(newLeader), disruptiveFollowerTermRef[0]);
    }


    @Test
    public void when_followerTerminatesInMinority_then_clusterRemainsAvailable() throws Exception {
        group = newGroup(3);
        group.start();
        RaftNodeImpl leaderNode = group.waitUntilLeaderElected();

        int leaderIndex = group.getLeaderIndex();
        for (int i = 0; i < group.size(); i++) {
            if (i != leaderIndex) {
                group.terminateNode(i);
                break;
            }
        }

        String value = "value";
        Future future = leaderNode.replicate(new ApplyRaftRunnable(value));
        assertEquals(value, future.get());
    }

    @Test
    public void when_leaderTerminatesInMinority_then_clusterRemainsAvailable() throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leaderNode = group.waitUntilLeaderElected();
        RaftNodeImpl[] followers = group.getNodesExcept(leaderNode.getLocalMember());
        int leaderTerm = getTerm(leaderNode);

        group.terminateNode(leaderNode.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint newLeader = getLeaderMember(raftNode);
                assertNotNull(newLeader);
                assertNotEquals(leaderNode.getLocalMember(), newLeader);
            }
        });

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();
        assertTrue(getTerm(newLeader) > leaderTerm);

        String value = "value";
        Future future = newLeader.replicate(new ApplyRaftRunnable(value));
        assertEquals(value, future.get());
    }

    @Test
    public void when_leaderStaysInMajorityDuringSplit_thenItMergesBackSuccessfully() {
        group = new LocalRaftGroup(5);
        group.start();
        group.waitUntilLeaderElected();

        int[] split = group.createMinoritySplitIndexes(false);
        group.split(split);

        assertTrueEventually(() -> {
            for (int ix : split) {
                assertNull(getLeaderMember(group.getNode(ix)));
            }
        });

        group.merge();
        group.waitUntilLeaderElected();
    }

    @Test
    public void when_leaderStaysInMinorityDuringSplit_thenItMergesBackSuccessfully() {
        int nodeCount = 5;
        group = new LocalRaftGroup(nodeCount);
        group.start();
        RaftEndpoint leaderEndpoint = group.waitUntilLeaderElected().getLocalMember();

        int[] split = group.createMajoritySplitIndexes(false);
        Arrays.sort(split);
        group.split(split);

        assertTrueEventually(() -> {
            for (int ix : split) {
                RaftEndpoint newLeader = getLeaderMember(group.getNode(ix));
                assertNotNull(newLeader);
                assertNotEquals(leaderEndpoint, newLeader);
            }
        });

        assertTrueEventually(() -> {
            for (int i = 0; i < nodeCount; i++) {
                if (Arrays.binarySearch(split, i) < 0) {
                    assertNull(getLeaderMember(group.getNode(i)));
                }
            }
        });

        group.merge();
        group.waitUntilLeaderElected();
    }

    @Test
    public void when_leaderCrashes_then_theFollowerWithLongestLogBecomesLeader() throws Exception {
        group = newGroup(4);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new ApplyRaftRunnable("val1")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl nextLeader = followers[0];
        long commitIndex = getCommitIndex(leader);

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(commitIndex, getCommitIndex(raftNode));
            }
        });

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToMember(leader.getLocalMember(), followers[i].getLocalMember(), AppendRequest.class);
        }

        leader.replicate(new ApplyRaftRunnable("val2"));

        assertTrueEventually(() -> assertTrue(getLastLogOrSnapshotEntry(nextLeader).index() > commitIndex));

        assertTrueAllTheTime(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(commitIndex, getCommitIndex(raftNode));
            }
        }, 10);

        group.terminateNode(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertEquals(nextLeader.getLocalMember(), getLeaderMember(raftNode));
            }
        });

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertEquals(commitIndex, getCommitIndex(raftNode));
                assertTrue(getLastLogOrSnapshotEntry(raftNode).index() > commitIndex);
                RaftDataService service = group.getService(raftNode);
                assertEquals(1, service.size());
                assertEquals("val1", service.get(1));
                // val2 not committed yet
            }
        });
    }

    @Test
    public void when_followerBecomesLeaderWithUncommittedEntries_then_thoseEntriesAreCommittedWithANewEntryOfCurrentTerm() throws Exception {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new ApplyRaftRunnable("val1")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl nextLeader = followers[0];
        long commitIndex = getCommitIndex(leader);

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(commitIndex, getCommitIndex(raftNode));
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(nextLeader.getLocalMember(), leader.getLocalMember(), AppendSuccessResponse.class);

        leader.replicate(new ApplyRaftRunnable("val2"));

        assertTrueEventually(() -> assertTrue(getLastLogOrSnapshotEntry(nextLeader).index() > commitIndex));

        assertTrueAllTheTime(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(commitIndex, getCommitIndex(raftNode));
            }
        }, 10);

        group.terminateNode(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertEquals(nextLeader.getLocalMember(), getLeaderMember(raftNode));
            }
        });

        nextLeader.replicate(new ApplyRaftRunnable("val3"));

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                assertEquals(3, getCommitIndex(raftNode));
                assertEquals(3, getLastLogOrSnapshotEntry(raftNode).index());
                RaftDataService service = group.getService(raftNode);
                assertEquals(3, service.size());
                assertEquals("val1", service.get(1));
                assertEquals("val2", service.get(2));
                assertEquals("val3", service.get(3));
            }
        });
    }

    @Test
    public void when_leaderCrashes_then_theFollowerWithLongestLogMayNotBecomeLeaderIfItsLogIsNotMajority() throws Exception {
        group = newGroup(5);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new ApplyRaftRunnable("val1")).get();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        RaftNodeImpl followerWithLongestLog = followers[0];
        long commitIndex = getCommitIndex(leader);

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(commitIndex, getCommitIndex(raftNode));
            }
        });

        for (int i = 1; i < followers.length; i++) {
            group.dropMessagesToMember(leader.getLocalMember(), followers[i].getLocalMember(), AppendRequest.class);
        }

        leader.replicate(new ApplyRaftRunnable("val2"));

        assertTrueEventually(() -> assertTrue(getLastLogOrSnapshotEntry(followerWithLongestLog).index() > commitIndex));

        assertTrueAllTheTime(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(commitIndex, getCommitIndex(raftNode));
            }
        }, 10);

        group.dropMessagesToMember(followerWithLongestLog.getLocalMember(), followers[1].getLocalMember(), VoteRequest.class);
        group.dropMessagesToMember(followerWithLongestLog.getLocalMember(), followers[2].getLocalMember(), VoteRequest.class);

        group.terminateNode(leader.getLocalMember());

        RaftNodeImpl newLeader = group.waitUntilLeaderElected();

        // followerWithLongestLog has 2 entries, other 3 followers have 1 entry
        // and those 3 followers will elect a leader among themselves

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint newLeader1 = getLeaderMember(raftNode);
                assertNotEquals(leader.getLocalMember(), newLeader1);
                assertNotEquals(followerWithLongestLog.getLocalMember(), newLeader1);
            }
        });

        for (int i = 1; i < followers.length; i++) {
            assertEquals(commitIndex, getCommitIndex(followers[i]));
            assertEquals(commitIndex, getLastLogOrSnapshotEntry(followers[i]).index());
        }

        // followerWithLongestLog does not truncate its extra log entry until the new leader appends a new entry
        assertTrue(getLastLogOrSnapshotEntry(followerWithLongestLog).index() > commitIndex);

        newLeader.replicate(new ApplyRaftRunnable("val3")).get();

        assertTrueEventually(() -> {
            for (RaftNodeImpl follower : followers) {
                assertEquals(2, getCommitIndex(follower));
                RaftDataService service = group.getService(follower);
                assertEquals(2, service.size());
                assertEquals("val1", service.get(1));
                assertEquals("val3", service.get(2));
            }
        });

        assertEquals(2, getLastLogOrSnapshotEntry(followerWithLongestLog).index());
    }

    @Test
    public void when_leaderStaysInMinorityDuringSplit_then_itCannotCommitNewEntries() throws ExecutionException, InterruptedException {
        group = newGroup(3, newRaftConfigWithNoSnapshotting(100));
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        leader.replicate(new ApplyRaftRunnable("val1")).get();

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : group.getNodes()) {
                assertEquals(1, getCommitIndex(raftNode));
            }
        });

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        group.split(leader.getLocalMember());

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftEndpoint leaderEndpoint = getLeaderMember(raftNode);
                assertNotNull(leaderEndpoint);
                assertNotEquals(leader.getLocalMember(), leaderEndpoint);
            }
        });

        List<InternalCompletableFuture> isolatedFutures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            isolatedFutures.add(leader.replicate(new ApplyRaftRunnable("isolated" + i)));
        }

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
        for (InternalCompletableFuture f : isolatedFutures) {
            try {
                f.joinInternal();
                fail();
            } catch (LeaderDemotedException ignored) {
            }
        }

        assertTrueEventually(() -> {
            for (RaftNodeImpl raftNode : followers) {
                RaftDataService service = group.getService(raftNode);
                assertEquals(11, service.size());
                assertEquals("val1", service.get(1));
                for (int i = 0; i < 10; i++) {
                    assertEquals(("valNew" + i), service.get(i + 2));
                }
            }
        });
    }

    @Test
    public void when_thereAreTooManyInflightAppendedEntries_then_newAppendsAreRejected() {
        int uncommittedEntryCount = 10;
        RaftAlgorithmConfig config = new RaftAlgorithmConfig().setUncommittedEntryCountToRejectNewAppends(uncommittedEntryCount);
        group = newGroup(2, config);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();
        group.terminateNode(follower.getLocalMember());

        for (int i = 0; i < uncommittedEntryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i));
        }

        try {
            leader.replicate(new ApplyRaftRunnable("valFinal")).joinInternal();
            fail();
        } catch (CannotReplicateException ignored) {
        }
    }

    @Test
    public void when_leaderStaysInMinority_then_itDemotesItselfToFollower() {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        group.split(leader.getLocalMember());
        InternalCompletableFuture f = leader.replicate(new ApplyRaftRunnable("val"));

        try {
            f.joinInternal();
            fail();
        } catch (StaleAppendRequestException ignored) {
        }
    }

    @Test
    public void when_leaderDemotesToFollower_then_itShouldNotDeleteItsVote() {
        group = newGroup(3);
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();

        assertEquals(leader.getLocalMember(), getVotedFor(leader));

        group.split(leader.getLocalMember());

        assertTrueEventually(() -> assertEquals(RaftRole.FOLLOWER,  getRole(leader)));

        assertEquals(leader.getLocalMember(), getVotedFor(leader));
    }

    private static RaftAlgorithmConfig newRaftConfigWithNoSnapshotting(int maxEntryCount) {
        return new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(maxEntryCount * 2);
    }
}
