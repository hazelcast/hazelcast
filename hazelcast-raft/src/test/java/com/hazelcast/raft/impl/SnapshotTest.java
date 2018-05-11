package com.hazelcast.raft.impl;

import com.hazelcast.config.raft.RaftAlgorithmConfig;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.exception.StaleAppendRequestException;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.service.ApplyRaftRunnable;
import com.hazelcast.raft.impl.service.RaftDataService;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.raft.impl.RaftUtil.getMatchIndex;
import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.raft.impl.RaftUtil.newGroupWithService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SnapshotTest extends HazelcastTestSupport {

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
    public void when_commitLogAdvances_then_snapshotIsTaken() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftAlgorithmConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(entryCount, getCommitIndex(raftNode));
                    assertEquals(entryCount, getSnapshotEntry(raftNode).index());
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(entryCount, service.size());
                    for (int i = 0; i < entryCount; i++) {
                        assertEquals(("val" + i), service.get(i + 1));
                    }
                }
            }
        });
    }

    @Test
    public void when_snapshotIsTaken_then_nextEntryIsCommitted() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftAlgorithmConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(entryCount, getCommitIndex(raftNode));
                    assertEquals(entryCount, getSnapshotEntry(raftNode).index());
                }
            }
        });

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(entryCount + 1, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(entryCount + 1, service.size());
                    for (int i = 0; i < entryCount; i++) {
                        assertEquals(("val" + i), service.get(i + 1));
                    }
                    assertEquals("valFinal", service.get(51));
                }
            }
        });
    }

    @Test
    public void when_followerFallsTooFarBehind_then_itInstallsSnapshot() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftAlgorithmConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        final RaftNodeImpl slowFollower = followers[1];

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);

        for (int i = 0; i < entryCount; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(entryCount, getSnapshotEntry(leader).index());
            }
        });

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        group.resetAllDropRulesFrom(leader.getLocalMember());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(entryCount + 1, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(entryCount + 1, service.size());
                    for (int i = 0; i < entryCount; i++) {
                        assertEquals(("val" + i), service.get(i + 1));
                    }
                    assertEquals("valFinal", service.get(51));
                }
            }
        });
    }

    @Test
    public void when_followerMissesTheLastEntryThatGoesIntoTheSnapshot_then_itInstallsSnapshot() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftAlgorithmConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());
        final RaftNodeImpl slowFollower = followers[1];

        for (int i = 0; i < entryCount - 1; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl follower : group.getNodesExcept(leader.getLocalMember())) {
                    assertEquals(entryCount - 1, getMatchIndex(leader, follower.getLocalMember()));
                }
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), slowFollower.getLocalMember(), AppendRequest.class);

        leader.replicate(new ApplyRaftRunnable("val" + (entryCount - 1))).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(entryCount, getSnapshotEntry(leader).index());
            }
        });

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        group.resetAllDropRulesFrom(leader.getLocalMember());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(entryCount + 1, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(entryCount + 1, service.size());
                    for (int i = 0; i < entryCount; i++) {
                        assertEquals(("val" + i), service.get(i + 1));
                    }
                    assertEquals("valFinal", service.get(51));
                }
            }
        });
    }

    @Test
    public void when_isolatedLeaderAppendsEntries_then_itInvalidatesTheirFeaturesUponInstallSnapshot() throws ExecutionException, InterruptedException {
        final int entryCount = 50;
        RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftAlgorithmConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        for (int i = 0; i < 40; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(40, getCommitIndex(raftNode));
                }
            }
        });

        group.split(leader.getLocalMember());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : followers) {
                    RaftMember leaderEndpoint = getLeaderMember(raftNode);
                    assertNotNull(leaderEndpoint);
                    assertNotEquals(leader.getLocalMember(), leaderEndpoint);
                }
            }
        });

        List<Future> futures = new ArrayList<Future>();
        for (int i = 40; i < 45; i++) {
            Future f = leader.replicate(new ApplyRaftRunnable("isolated" + i));
            futures.add(f);
        }

        final RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));

        for (int i = 40; i < 51; i++) {
            newLeader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : followers) {
                    assertTrue(getSnapshotEntry(raftNode).index() > 0);
                }
            }
        });

        group.dropMessagesToMember(leader.getLocalMember(), followers[0].getLocalMember(), AppendRequest.class);
        group.dropMessagesToMember(leader.getLocalMember(), followers[1].getLocalMember(), AppendRequest.class);
        group.merge();

        for (Future f : futures) {
            try {
                f.get();
                fail();
            } catch (StaleAppendRequestException ignored) {
            }
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(51, getCommitIndex(raftNode));
                    RaftDataService service = group.getService(raftNode);
                    assertEquals(51, service.size());
                    for (int i = 0; i < 51; i++) {
                        assertEquals(("val" + i), service.get(i + 1));
                    }

                }
            }
        });
    }
}
