/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cp.internal.session;

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.AbstractProxySessionManagerTest;
import com.hazelcast.cp.internal.session.SessionAwareProxy;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSessionManagerTest extends AbstractProxySessionManagerTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private HazelcastInstance client;

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Before
    public void setupClient() {
        TestHazelcastFactory f = (TestHazelcastFactory) factory;
        client = f.newHazelcastClient();
    }

    @Test
    public void sessionHeartbeatsAreStopped_afterClusterRestart() {
        ClientProxySessionManager sessionManager = (ClientProxySessionManager) getSessionManager();
        long sessionId = sessionManager.acquireSession(groupId);

        AtomicInteger heartbeatCounter = new AtomicInteger();
        doAnswer(invocation -> {
            heartbeatCounter.incrementAndGet();
            return invocation.callRealMethod();
        }).when(sessionManager).heartbeat(groupId, sessionId);

        assertTrueEventually(() -> verify(sessionManager, atLeastOnce()).heartbeat(groupId, sessionId));

        Address address1 = members[0].getCPSubsystem().getLocalCPMember().getAddress();
        Address address2 = members[1].getCPSubsystem().getLocalCPMember().getAddress();
        Address address3 = members[2].getCPSubsystem().getLocalCPMember().getAddress();

        members[0].getLifecycleService().terminate();
        members[1].getLifecycleService().terminate();
        members[2].getLifecycleService().terminate();

        Config config = createConfig(3, 3);
        HazelcastInstance instance1 = factory.newHazelcastInstance(address1, config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(address2, config);
        HazelcastInstance instance3 = factory.newHazelcastInstance(address3, config);

        waitUntilCPDiscoveryCompleted(instance1, instance2, instance3);

        assertTrueEventually(() -> verify(sessionManager, times(1)).invalidateSession(groupId, sessionId));
        int heartbeatCount = heartbeatCounter.get();
        assertTrueAllTheTime(() -> verify(sessionManager, times(heartbeatCount)).heartbeat(groupId, sessionId), 3);
    }

    @Test
    public void testClientSessionManagerShutdown() throws ExecutionException, InterruptedException {
        AbstractProxySessionManager sessionManager = getSessionManager();
        SessionProxyImpl proxy = new SessionProxyImpl(sessionManager, groupId);
        proxy.createSession();

        Map<RaftGroupId, InternalCompletableFuture<Object>> futures = sessionManager.shutdown();
        assertEquals(1, futures.size());

        Entry<RaftGroupId, InternalCompletableFuture<Object>> e = futures.entrySet().iterator().next();
        assertEquals(groupId, e.getKey());
        e.getValue().get();

        exception.expect(IllegalStateException.class);
        proxy.createSession();
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }

    protected AbstractProxySessionManager getSessionManager() {
        return spy((((HazelcastClientProxy) client).client).getProxySessionManager());
    }

    private static class SessionProxyImpl extends SessionAwareProxy {

        SessionProxyImpl(AbstractProxySessionManager sessionManager, RaftGroupId groupId) {
            super(sessionManager, groupId);
        }

        long createSession() {
            return super.acquireSession();
        }
    }
}
