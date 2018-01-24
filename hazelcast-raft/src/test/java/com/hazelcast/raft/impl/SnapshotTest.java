package com.hazelcast.raft.impl;

import com.hazelcast.config.raft.RaftConfig;
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
import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
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
        RaftConfig raftConfig = new RaftConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftConfig);
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
        RaftConfig raftConfig = new RaftConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftConfig);
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
        RaftConfig raftConfig = new RaftConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        final RaftNodeImpl slowFollower = followers[1];

        group.dropMessagesToEndpoint(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendRequest.class);

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

        group.resetAllDropRulesFrom(leader.getLocalEndpoint());

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
        RaftConfig raftConfig = new RaftConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();

        RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());
        final RaftNodeImpl slowFollower = followers[1];

        for (int i = 0; i < entryCount - 1; i++) {
            leader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl follower : group.getNodesExcept(leader.getLocalEndpoint())) {
                    assertEquals(entryCount - 1, getMatchIndex(leader, follower.getLocalEndpoint()));
                }
            }
        });

        group.dropMessagesToEndpoint(leader.getLocalEndpoint(), slowFollower.getLocalEndpoint(), AppendRequest.class);

        leader.replicate(new ApplyRaftRunnable("val" + (entryCount - 1))).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(entryCount, getSnapshotEntry(leader).index());
            }
        });

        leader.replicate(new ApplyRaftRunnable("valFinal")).get();

        group.resetAllDropRulesFrom(leader.getLocalEndpoint());

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
        RaftConfig raftConfig = new RaftConfig().setCommitIndexAdvanceCountToSnapshot(entryCount);
        group = newGroupWithService(3, raftConfig);
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalEndpoint());

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

        List<Future> futures = new ArrayList<Future>();
        for (int i = 40; i < 45; i++) {
            Future f = leader.replicate(new ApplyRaftRunnable("isolated" + i));
            futures.add(f);
        }

        final RaftNodeImpl newLeader = group.getNode(getLeaderEndpoint(followers[0]));

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

        group.dropMessagesToEndpoint(leader.getLocalEndpoint(), followers[0].getLocalEndpoint(), AppendRequest.class);
        group.dropMessagesToEndpoint(leader.getLocalEndpoint(), followers[1].getLocalEndpoint(), AppendRequest.class);
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
