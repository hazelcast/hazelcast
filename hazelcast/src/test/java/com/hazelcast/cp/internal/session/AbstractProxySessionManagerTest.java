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

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.ChangeLoggingRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class AbstractProxySessionManagerTest extends HazelcastRaftTestSupport {

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-debug-cp.xml");

    private static final int sessionTTLSeconds = 10;

    HazelcastInstance[] members;
    protected RaftGroupId groupId;

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        members = newInstances(3);
        RaftInvocationManager invocationManager = getRaftInvocationManager(members[0]);
        groupId = invocationManager.createRaftGroup("group").get();
    }

    @Test
    public void getSession_returnsNoSessionId_whenNoSessionCreated() {
        AbstractProxySessionManager sessionManager = getSessionManager();
        assertEquals(NO_SESSION_ID, sessionManager.getSession(groupId));
    }

    @Test
    public void acquireSession_createsNewSession_whenSessionNotExists() {
        AbstractProxySessionManager sessionManager = getSessionManager();
        long sessionId = sessionManager.acquireSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);
        assertEquals(sessionId, sessionManager.getSession(groupId));
        assertEquals(1, sessionManager.getSessionAcquireCount(groupId, sessionId));

        SessionAccessor sessionAccessor = getSessionAccessor();
        assertTrueEventually(() -> assertTrue(sessionAccessor.isActive(groupId, sessionId)));
    }

    @Test
    public void acquireSession_returnsExistingSession_whenSessionExists() {
        AbstractProxySessionManager sessionManager = getSessionManager();
        long newSessionId = sessionManager.acquireSession(groupId);
        long sessionId = sessionManager.acquireSession(groupId);
        assertEquals(newSessionId, sessionId);
        assertEquals(sessionId, sessionManager.getSession(groupId));
        assertEquals(2, sessionManager.getSessionAcquireCount(groupId, sessionId));
    }

    @Test
    public void acquireSession_returnsTheSameSessionId_whenExecutedConcurrently() throws Exception {
        AbstractProxySessionManager sessionManager = getSessionManager();

        Callable<Long> acquireSessionCall = () -> sessionManager.acquireSession(groupId);

        Future<Long>[] futures = new Future[5];
        for (int i = 0; i < futures.length; i++) {
            futures[i] = spawn(acquireSessionCall);
        }

        long[] sessions = new long[futures.length];
        for (int i = 0; i < futures.length; i++) {
            sessions[i] = futures[i].get();
        }

        long expectedSessionId = sessionManager.getSession(groupId);
        for (long sessionId : sessions) {
            assertEquals(expectedSessionId, sessionId);
        }
        assertEquals(sessions.length, sessionManager.getSessionAcquireCount(groupId, expectedSessionId));
    }

    @Test
    public void releaseSession_hasNoEffect_whenSessionNotExists() {
        AbstractProxySessionManager sessionManager = getSessionManager();
        sessionManager.releaseSession(groupId, 1);
    }

    @Test
    public void releaseSession_whenSessionExists() {
        AbstractProxySessionManager sessionManager = getSessionManager();
        long sessionId = sessionManager.acquireSession(groupId);
        sessionManager.releaseSession(groupId, sessionId);
        assertEquals(0, sessionManager.getSessionAcquireCount(groupId, sessionId));
    }

    @Test
    public void sessionHeartbeatsAreNotSent_whenSessionNotExists() {
        AbstractProxySessionManager sessionManager = getSessionManager();
        long sessionId = 1;

        assertTrueAllTheTime(() -> verify(sessionManager, never()).heartbeat(groupId, sessionId), 5);
    }

    @Test
    public void sessionHeartbeatsAreSent_whenSessionInUse() {
        AbstractProxySessionManager sessionManager = getSessionManager();
        long sessionId = sessionManager.acquireSession(groupId);

        SessionAccessor sessionAccessor = getSessionAccessor();
        int heartbeatCount = 5;
        for (int i = 0; i < heartbeatCount; i++) {
            int times = i + 1;
            assertTrueEventually(() -> verify(sessionManager, atLeast(times)).heartbeat(groupId, sessionId));
            assertTrue(sessionAccessor.isActive(groupId, sessionId));
        }
    }

    @Test
    public void sessionHeartbeatsAreNotSent_whenSessionReleased() {
        AbstractProxySessionManager sessionManager = getSessionManager();
        long sessionId = sessionManager.acquireSession(groupId);

        assertTrueEventually(() -> verify(sessionManager, atLeastOnce()).heartbeat(groupId, sessionId));

        sessionManager.releaseSession(groupId, sessionId);

        SessionAccessor sessionAccessor = getSessionAccessor();
        assertTrueEventually(() -> assertFalse(sessionAccessor.isActive(groupId, sessionId)));
    }

    @Test
    public void acquireSession_returnsTheExistingSession_whenSessionInUse() {
        AbstractProxySessionManager sessionManager = getSessionManager();

        long sessionId = sessionManager.acquireSession(groupId);

        when(sessionManager.heartbeat(groupId, sessionId)).thenReturn(completedFuture());

        SessionAccessor sessionAccessor = getSessionAccessor();
        assertTrueEventually(() -> assertFalse(sessionAccessor.isActive(groupId, sessionId)));

        assertTrueAllTheTime(() -> assertEquals(sessionId, sessionManager.acquireSession(groupId)), 3);
    }

    @Test
    public void acquireSession_returnsNewSession_whenSessionExpiredAndNotInUse() {
        AbstractProxySessionManager sessionManager = getSessionManager();

        long sessionId = sessionManager.acquireSession(groupId);

        when(sessionManager.heartbeat(groupId, sessionId)).thenReturn(completedFuture());

        SessionAccessor sessionAccessor = getSessionAccessor();
        assertTrueEventually(() -> assertFalse(sessionAccessor.isActive(groupId, sessionId)));

        sessionManager.releaseSession(groupId, sessionId);

        assertTrueEventually(() -> {
            long newSessionId = sessionManager.acquireSession(groupId);
            sessionManager.releaseSession(groupId, newSessionId);
            assertNotEquals(sessionId, newSessionId);
        });
    }

    protected abstract AbstractProxySessionManager getSessionManager();

    private InternalCompletableFuture<Object> completedFuture() {
        return InternalCompletableFuture.newCompletedFuture(null, CALLER_RUNS);
    }

    private SessionAccessor getSessionAccessor() {
        HazelcastInstance leaderInstance = getLeaderInstance(members, groupId);
        return getNodeEngineImpl(leaderInstance).getService(RaftSessionService.SERVICE_NAME);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setSessionHeartbeatIntervalSeconds(1);
        cpSubsystemConfig.setSessionTimeToLiveSeconds(sessionTTLSeconds);
        return config;
    }
}
