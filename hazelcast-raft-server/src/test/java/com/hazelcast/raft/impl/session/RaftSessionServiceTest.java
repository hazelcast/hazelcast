package com.hazelcast.raft.impl.session;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.service.RaftTestApplyOp;
import com.hazelcast.raft.impl.session.operation.CloseSessionOp;
import com.hazelcast.raft.impl.session.operation.CreateSessionOp;
import com.hazelcast.raft.impl.session.operation.HeartbeatSessionOp;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftSessionServiceTest extends HazelcastRaftTestSupport {

    private static final String RAFT_GROUP_NAME = "sessions";
    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private HazelcastInstance[] instances;
    private RaftInvocationManager invocationManager;
    private RaftGroupId groupId;

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        int raftGroupSize = 3;
        instances = newInstances(raftGroupSize);
        invocationManager = getRaftInvocationManager(instances[0]);
        groupId = invocationManager.createRaftGroup(RAFT_GROUP_NAME, raftGroupSize).get();
    }

    @Test
    public void testSessionCreate() throws ExecutionException, InterruptedException {
        final SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();
        System.out.println(response);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    assertNotNull(registry.getSession(response.getSessionId()));
                }
            }
        });
    }

    @Test
    public void testSessionHeartbeat() throws ExecutionException, InterruptedException {
        final SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();
        final Session[] sessions = new Session[instances.length];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length; i++) {
                    RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    Session session = registry.getSession(response.getSessionId());
                    assertNotNull(session);
                    sessions[i] = session;
                }
            }
        });

        invocationManager.invoke(groupId, new HeartbeatSessionOp(response.getSessionId())).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length; i++) {
                    RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    Session session = registry.getSession(response.getSessionId());
                    assertNotNull(session);
                    assertTrue(session.version() > sessions[i].version());
                }
            }
        });
    }

    @Test
    public void testSessionClose() throws ExecutionException, InterruptedException {
        final SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    assertNotNull(registry.getSession(response.getSessionId()));
                }
            }
        });

        invocationManager.invoke(groupId, new CloseSessionOp(response.getSessionId())).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    assertNull(registry.getSession(response.getSessionId()));
                }
            }
        });
    }

    @Test
    public void testHeartbeatFailsAfterSessionClose() throws ExecutionException, InterruptedException {
        final SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    assertNotNull(registry.getSession(response.getSessionId()));
                }
            }
        });

        invocationManager.invoke(groupId, new CloseSessionOp(response.getSessionId())).get();

        exception.expectCause(isA(SessionExpiredException.class));

        invocationManager.invoke(groupId, new HeartbeatSessionOp(response.getSessionId())).get();
    }

    @Test
    public void testLeaderFailureShiftsSessionExpirationTimes() throws ExecutionException, InterruptedException {
        final SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();
        final Session[] sessions = new Session[instances.length];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length; i++) {
                    RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    Session session = registry.getSession(response.getSessionId());
                    assertNotNull(session);
                    sessions[i] = session;
                }
            }
        });

        RaftMemberImpl leaderEndpoint = getLeaderMember(getRaftNode(instances[0], groupId));
        final HazelcastInstance leader = factory.getInstance(leaderEndpoint.getAddress());
        leader.getLifecycleService().terminate();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < instances.length; i++) {
                    Node node = getNode(instances[i]);
                    if (node == null) {
                        continue;
                    }

                    RaftSessionService service = node.nodeEngine.getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    Session session = registry.getSession(response.getSessionId());
                    assertNotNull(session);
                    assertTrue(session.version() > sessions[i].version());
                }
            }
        });
    }

    @Test
    public void testSessionHeartbeatTimeout() throws ExecutionException, InterruptedException {
        final SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    Session session = registry.getSession(response.getSessionId());
                    assertNotNull(session);
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                    SessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                    assertNotNull(registry);
                    Session session = registry.getSession(response.getSessionId());
                    assertNull(session);
                }
            }
        });
    }

    @Test
    public void testSnapshotRestore() throws ExecutionException, InterruptedException {
        final HazelcastInstance leader = getLeaderInstance(instances, groupId);
        final HazelcastInstance follower = getRandomFollowerInstance(instances, groupId);

        // the follower falls behind the leader. It neither append entries nor installs snapshots.
        dropOperationsBetween(leader, follower, RaftServiceDataSerializerHook.F_ID, asList(RaftServiceDataSerializerHook.APPEND_REQUEST_OP, RaftServiceDataSerializerHook.INSTALL_SNAPSHOT_OP));

        final SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, new CreateSessionOp()).get();

        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            invocationManager.invoke(groupId, new RaftTestApplyOp("value" + i)).get();
        }

        final RaftNodeImpl leaderRaftNode = (RaftNodeImpl) ((RaftService) getNodeEngineImpl(leader).getService(RaftService.SERVICE_NAME)).getRaftNode(groupId);
        final RaftNodeImpl followerRaftNode = (RaftNodeImpl) ((RaftService) getNodeEngineImpl(follower).getService(RaftService.SERVICE_NAME)).getRaftNode(groupId);

        // the leader takes a snapshot
        final long[] leaderSnapshotIndex = new long[1];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                long idx = getSnapshotEntry(leaderRaftNode).index();
                assertTrue(idx > 0);
                leaderSnapshotIndex[0] = idx;
            }
        });

        // the follower doesn't have it since its raft log is still empty
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, getSnapshotEntry(followerRaftNode).index());
            }
        }, 10);

        resetPacketFiltersFrom(leader);

        // the follower installs the snapshot after it hears from the leader
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getSnapshotEntry(followerRaftNode).index() > 0);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSessionService sessionService = getNodeEngineImpl(follower).getService(RaftSessionService.SERVICE_NAME);
                SessionRegistry registry = sessionService.getSessionRegistryOrNull(groupId);
                assertNotNull(registry.getSession(response.getSessionId()));
            }
        });

        // the follower disconnects from the leader again
        dropOperationsBetween(leader, follower, RaftServiceDataSerializerHook.F_ID, asList(RaftServiceDataSerializerHook.APPEND_REQUEST_OP, RaftServiceDataSerializerHook.INSTALL_SNAPSHOT_OP));

        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            invocationManager.invoke(groupId, new HeartbeatSessionOp(response.getSessionId())).get();
        }

        // the leader takes a new snapshot
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getSnapshotEntry(leaderRaftNode).index() > leaderSnapshotIndex[0]);
            }
        });

        resetPacketFiltersFrom(leader);

        // the follower installs the new snapshot after it hears from the leader
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Session leaderSession = getSession(leader, groupId, response.getSessionId());
                Session followerSession = getSession(follower, groupId, response.getSessionId());

                assertNotNull(leaderSession);
                assertNotNull(followerSession);

                assertEquals(leaderSession.version(), followerSession.version());
            }
        });
    }

    @Override
    protected Config createConfig(int groupSize, int metadataGroupSize) {
        Config config = super.createConfig(groupSize, metadataGroupSize);
        RaftConfig raftConfig = config.getRaftConfig();
        raftConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        raftConfig.addGroupConfig(new RaftGroupConfig(RAFT_GROUP_NAME, 3));
        return config;
    }

    private Session getSession(HazelcastInstance instance, RaftGroupId groupId, long sessionId) {
        RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
        SessionRegistry registry = sessionService.getSessionRegistryOrNull(groupId);
        if (registry == null) {
            return null;
        }

        return registry.getSession(sessionId);
    }
}
