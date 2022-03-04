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

package com.hazelcast.cp.internal.session;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.RaftTestApplyOp;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.session.operation.CloseSessionOp;
import com.hazelcast.cp.internal.session.operation.CreateSessionOp;
import com.hazelcast.cp.internal.session.operation.HeartbeatSessionOp;
import com.hazelcast.cp.session.CPSession;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.cp.session.CPSession.CPSessionOwnerType.SERVER;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftSessionServiceTest extends HazelcastRaftTestSupport {

    private static final String RAFT_GROUP_NAME = "sessions";
    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private HazelcastInstance[] instances;
    private RaftInvocationManager invocationManager;
    private CPGroupId groupId;

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        int groupSize = 3;
        instances = newInstances(groupSize);
        invocationManager = getRaftInvocationManager(instances[0]);
        groupId = invocationManager.createRaftGroup(RAFT_GROUP_NAME, groupSize).get();
    }

    @Test
    public void testSessionCreate() throws ExecutionException, InterruptedException, UnknownHostException {
        SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, newCreateSessionOp()).get();
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                RaftSessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                assertNotNull(registry);

                CPSession session = registry.getSession(response.getSessionId());
                assertNotNull(session);

                Collection<CPSession> sessions = service.getAllSessions(groupId).get();
                assertThat(sessions, hasItem(session));
            }
        });
    }

    @Test
    public void testSessionHeartbeat() throws ExecutionException, InterruptedException, UnknownHostException {
        SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, newCreateSessionOp()).get();
        CPSessionInfo[] sessions = new CPSessionInfo[instances.length];
        assertTrueEventually(() -> {
            for (int i = 0; i < instances.length; i++) {
                RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                RaftSessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                assertNotNull(registry);
                CPSessionInfo session = registry.getSession(response.getSessionId());
                assertNotNull(session);
                sessions[i] = session;
            }
        });

        invocationManager.invoke(groupId, new HeartbeatSessionOp(response.getSessionId())).get();

        assertTrueEventually(() -> {
            for (int i = 0; i < instances.length; i++) {
                RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                RaftSessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                assertNotNull(registry);
                CPSessionInfo session = registry.getSession(response.getSessionId());
                assertNotNull(session);
                assertTrue(session.version() > sessions[i].version());
            }
        });
    }

    @Test
    public void testSessionClose() throws ExecutionException, InterruptedException, UnknownHostException {
        SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, newCreateSessionOp()).get();
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                RaftSessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                assertNotNull(registry);
                assertNotNull(registry.getSession(response.getSessionId()));
            }
        });

        invocationManager.invoke(groupId, new CloseSessionOp(response.getSessionId())).get();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                RaftSessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                assertNotNull(registry);
                assertNull(registry.getSession(response.getSessionId()));
                assertThat(service.getAllSessions(groupId).get(), empty());
            }
        });
    }

    @Test
    public void testHeartbeatFailsAfterSessionClose() throws ExecutionException, InterruptedException, UnknownHostException {
        SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, newCreateSessionOp()).get();
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                RaftSessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                assertNotNull(registry);
                assertNotNull(registry.getSession(response.getSessionId()));
            }
        });

        invocationManager.invoke(groupId, new CloseSessionOp(response.getSessionId())).get();

        exception.expectCause(isA(SessionExpiredException.class));

        invocationManager.invoke(groupId, new HeartbeatSessionOp(response.getSessionId())).get();
    }

    @Test
    public void testLeaderFailureShiftsSessionExpirationTimes() throws ExecutionException, InterruptedException, UnknownHostException {
        SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, newCreateSessionOp()).get();
        CPSessionInfo[] sessions = new CPSessionInfo[instances.length];
        assertTrueEventually(() -> {
            for (int i = 0; i < instances.length; i++) {
                RaftSessionService service = getNodeEngineImpl(instances[i]).getService(RaftSessionService.SERVICE_NAME);
                RaftSessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                assertNotNull(registry);
                CPSessionInfo session = registry.getSession(response.getSessionId());
                assertNotNull(session);
                sessions[i] = session;
            }
        });

        RaftEndpoint leaderEndpoint = getLeaderMember(getRaftNode(instances[0], groupId));
        HazelcastInstance leader = getInstance(leaderEndpoint);
        leader.getLifecycleService().terminate();

        assertTrueEventually(() -> {
            for (int i = 0; i < instances.length; i++) {
                Node node;
                try {
                     node = getNode(instances[i]);
                } catch (IllegalArgumentException ignored) {
                    continue;
                }

                RaftSessionService service = node.nodeEngine.getService(RaftSessionService.SERVICE_NAME);
                RaftSessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                assertNotNull(registry);
                CPSessionInfo session = registry.getSession(response.getSessionId());
                assertNotNull(session);
                assertTrue(session.version() > sessions[i].version());
            }
        });
    }

    @Test
    public void testSessionHeartbeatTimeout() throws ExecutionException, InterruptedException, UnknownHostException {
        SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, newCreateSessionOp()).get();
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                RaftSessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                assertNotNull(registry);
                CPSessionInfo session = registry.getSession(response.getSessionId());
                assertNotNull(session);
            }
        });

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                RaftSessionRegistry registry = service.getSessionRegistryOrNull(groupId);
                assertNotNull(registry);
                CPSessionInfo session = registry.getSession(response.getSessionId());
                assertNull(session);
            }
        });
    }

    @Test
    public void testSnapshotRestore() throws ExecutionException, InterruptedException, UnknownHostException {
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        HazelcastInstance follower = getRandomFollowerInstance(instances, groupId);

        // the follower falls behind the leader. It neither append entries nor installs snapshots.
        dropOperationsBetween(leader, follower, RaftServiceDataSerializerHook.F_ID, asList(RaftServiceDataSerializerHook.APPEND_REQUEST_OP, RaftServiceDataSerializerHook.INSTALL_SNAPSHOT_OP));

        SessionResponse response = invocationManager.<SessionResponse>invoke(groupId, newCreateSessionOp()).get();

        spawn(() -> {
            for (int i = 0; i < 30; i++) {
                invocationManager.invoke(groupId, new HeartbeatSessionOp(response.getSessionId())).joinInternal();
                sleepAtLeastSeconds(5);
            }
        });

        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            invocationManager.invoke(groupId, new RaftTestApplyOp("value" + i)).get();
        }

        RaftNodeImpl leaderRaftNode = (RaftNodeImpl) ((RaftService) getNodeEngineImpl(leader).getService(RaftService.SERVICE_NAME)).getRaftNode(groupId);
        RaftNodeImpl followerRaftNode = (RaftNodeImpl) ((RaftService) getNodeEngineImpl(follower).getService(RaftService.SERVICE_NAME)).getRaftNode(groupId);

        // the leader takes a snapshot
        long[] leaderSnapshotIndex = new long[1];
        assertTrueEventually(() -> {
            long idx = getSnapshotEntry(leaderRaftNode).index();
            assertTrue(idx > 0);
            leaderSnapshotIndex[0] = idx;
        });

        // the follower doesn't have it since its raft log is still empty
        assertTrueAllTheTime(() -> assertEquals(0, getSnapshotEntry(followerRaftNode).index()), 10);

        resetPacketFiltersFrom(leader);

        // the follower installs the snapshot after it hears from the leader
        assertTrueEventually(() -> assertTrue(getSnapshotEntry(followerRaftNode).index() > 0));

        assertTrueEventually(() -> {
            RaftSessionService sessionService = getNodeEngineImpl(follower).getService(RaftSessionService.SERVICE_NAME);
            RaftSessionRegistry registry = sessionService.getSessionRegistryOrNull(groupId);
            assertNotNull(registry.getSession(response.getSessionId()));
        });

        // the follower disconnects from the leader again
        dropOperationsBetween(leader, follower, RaftServiceDataSerializerHook.F_ID, asList(RaftServiceDataSerializerHook.APPEND_REQUEST_OP, RaftServiceDataSerializerHook.INSTALL_SNAPSHOT_OP));

        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            invocationManager.invoke(groupId, new HeartbeatSessionOp(response.getSessionId())).get();
        }

        // the leader takes a new snapshot
        assertTrueEventually(() -> assertTrue(getSnapshotEntry(leaderRaftNode).index() > leaderSnapshotIndex[0]));

        resetPacketFiltersFrom(leader);

        // the follower installs the new snapshot after it hears from the leader
        assertTrueEventually(() -> {
            CPSessionInfo leaderSession = getSession(leader, groupId, response.getSessionId());
            CPSessionInfo followerSession = getSession(follower, groupId, response.getSessionId());

            assertNotNull(leaderSession);
            assertNotNull(followerSession);

            assertEquals(leaderSession.version(), followerSession.version());
        });
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.getCPSubsystemConfig()
              .setSessionTimeToLiveSeconds(20)
              .getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);

        return config;
    }

    private CPSessionInfo getSession(HazelcastInstance instance, CPGroupId groupId, long sessionId) {
        RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
        RaftSessionRegistry registry = sessionService.getSessionRegistryOrNull(groupId);
        if (registry == null) {
            return null;
        }

        return registry.getSession(sessionId);
    }

    private CreateSessionOp newCreateSessionOp() throws UnknownHostException {
        return new CreateSessionOp(new Address("localhost", 1111), "server1", SERVER);
    }
}
